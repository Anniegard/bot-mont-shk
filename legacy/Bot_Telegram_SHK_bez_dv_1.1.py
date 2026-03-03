import os
import re
import asyncio
import logging

import gspread
import pandas as pd
from google.oauth2.service_account import Credentials
from telegram import InlineKeyboardButton, InlineKeyboardMarkup, Update
from telegram.ext import (
    Application,
    CallbackContext,
    CallbackQueryHandler,
    CommandHandler,
    MessageHandler,
    filters,
)

SCOPE = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
]

EXPORT_WITHOUT_TRANSFERS = "export_without_transfers"
EXPORT_WITH_TRANSFERS = "export_with_transfers"
EXPORT_ONLY_TRANSFERS = "export_only_transfers"

MAX_UPLOAD_SIZE_BYTES = 20 * 1024 * 1024
REQUIRED_COLUMNS = {"Гофра", "ШК", "Стоимость"}


def load_env_file(path: str):
    if not path or not os.path.exists(path):
        return
    with open(path, "r", encoding="utf-8") as f:
        for raw_line in f:
            line = raw_line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            key, value = line.split("=", 1)
            key = key.strip()
            value = value.strip().strip('"').strip("'")
            os.environ.setdefault(key, value)


BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DEFAULT_ENV_FILE = os.path.join(BASE_DIR, ".env")
CONFIG_ENV_FILE = os.getenv("BOT_CONFIG_FILE", DEFAULT_ENV_FILE)
load_env_file(CONFIG_ENV_FILE)

TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "")
GOOGLE_CREDENTIALS_PATH = os.getenv("GOOGLE_CREDENTIALS_PATH", "")
SPREADSHEET_ID = os.getenv("SPREADSHEET_ID", "")
ADMIN_USER_ID = os.getenv("ADMIN_USER_ID", "")

if not TELEGRAM_BOT_TOKEN or not GOOGLE_CREDENTIALS_PATH or not SPREADSHEET_ID:
    raise EnvironmentError(
        "Missing config: TELEGRAM_BOT_TOKEN, GOOGLE_CREDENTIALS_PATH, SPREADSHEET_ID"
    )


class SensitiveDataFilter(logging.Filter):
    def __init__(self, token: str):
        super().__init__()
        self.patterns = []
        if token:
            self.patterns = [
                token,
                f"bot{token}",
                re.escape(token),
            ]

    def filter(self, record: logging.LogRecord) -> bool:
        message = record.getMessage()
        redacted = message
        for pattern in self.patterns:
            redacted = redacted.replace(pattern, "[REDACTED]")
        record.msg = redacted
        record.args = ()
        return True


def setup_logging() -> None:
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)

    formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
    stream_handler = logging.StreamHandler()
    file_handler = logging.FileHandler(
        os.path.join(BASE_DIR, "bot_logs.txt"), mode="a", encoding="utf-8"
    )

    stream_handler.setFormatter(formatter)
    file_handler.setFormatter(formatter)

    redaction_filter = SensitiveDataFilter(TELEGRAM_BOT_TOKEN)
    stream_handler.addFilter(redaction_filter)
    file_handler.addFilter(redaction_filter)

    logger.handlers = [stream_handler, file_handler]


def authorize_google_credentials(credentials_path: str):
    try:
        creds = Credentials.from_service_account_file(credentials_path, scopes=SCOPE)
        return gspread.authorize(creds)
    except Exception as e:
        logging.error("Google authorization error: %s", e)
        raise


class TelegramBot:
    def __init__(self, token: str, credentials_path: str, spreadsheet_id: str):
        self.token = token
        self.credentials_path = credentials_path
        self.spreadsheet_id = spreadsheet_id
        self.application = Application.builder().token(self.token).build()
        self.gc = authorize_google_credentials(self.credentials_path)
        self.processing_lock = asyncio.Lock()
        self.setup_handlers()

    def setup_handlers(self):
        self.application.add_handler(CommandHandler("start", self.start))
        self.application.add_handler(CommandHandler("admin", self.admin))
        self.application.add_handler(
            MessageHandler(
                filters.Document.MimeType(
                    "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
                ),
                self.handle_file,
            )
        )
        self.application.add_handler(CallbackQueryHandler(self.admin_button_handler))

    async def start(self, update: Update, context: CallbackContext):
        await update.message.reply_text(
            "Привет! Отправь мне файл с выгрузкой, и я загружу данные в Google Sheets."
        )

    async def admin(self, update: Update, context: CallbackContext):
        if ADMIN_USER_ID and str(update.message.from_user.id) == ADMIN_USER_ID:
            admin_keyboard = [
                [InlineKeyboardButton("Просмотреть логи", callback_data="view_logs")],
                [
                    InlineKeyboardButton(
                        "Запустить новую задачу", callback_data="start_task"
                    )
                ],
                [InlineKeyboardButton("Остановить бота", callback_data="stop_bot")],
            ]
            await update.message.reply_text(
                "Выберите действие:", reply_markup=InlineKeyboardMarkup(admin_keyboard)
            )
        else:
            await update.message.reply_text(
                "У вас нет прав для использования этой команды."
            )

    async def admin_button_handler(self, update: Update, context: CallbackContext):
        query = update.callback_query
        await query.answer()

        if query.data in {
            EXPORT_WITHOUT_TRANSFERS,
            EXPORT_WITH_TRANSFERS,
            EXPORT_ONLY_TRANSFERS,
        }:
            await self.process_export_mode(query, context)
            return

        if query.data == "cancel":
            await self.cleanup_pending_file(context)
            await query.message.reply_text("Загрузка отменена.")
            return

        if query.data == "view_logs":
            log_path = os.path.join(BASE_DIR, "bot_logs.txt")
            with open(log_path, "r", encoding="utf-8") as log_file:
                logs = log_file.readlines()
            await query.message.reply_text(f"Последние логи:\n{''.join(logs[-10:])}")
            return

        if query.data == "start_task":
            await query.message.reply_text("Загрузите файл и выберите режим выгрузки.")
            return

        if query.data == "stop_bot":
            if not ADMIN_USER_ID or str(query.from_user.id) != ADMIN_USER_ID:
                await query.message.reply_text(
                    "Остановка доступна только администратору."
                )
                return
            await query.message.reply_text("Останавливаю бота...")
            await context.application.stop()

    async def handle_file(self, update: Update, context: CallbackContext):
        logging.info(
            "Новый запрос на обработку файла от %s", update.message.from_user.username
        )

        if self.processing_lock.locked():
            await update.message.reply_text(
                "Сейчас выполняется другая выгрузка. Повторите чуть позже."
            )
            return

        document = update.message.document
        if document.file_size and document.file_size > MAX_UPLOAD_SIZE_BYTES:
            await update.message.reply_text(
                f"Файл слишком большой. Максимум: {MAX_UPLOAD_SIZE_BYTES // (1024 * 1024)} МБ."
            )
            return

        await self.cleanup_pending_file(context)

        try:
            tg_file = await document.get_file()
            file_name = f"uploaded_file_{update.message.from_user.id}.xlsx"
            target_path = os.path.join(BASE_DIR, file_name)
            file_path = await tg_file.download_to_drive(target_path)
            context.user_data["pending_file_path"] = file_path
            logging.info("Файл загружен: %s", file_path)
        except Exception as e:
            logging.error("Ошибка при загрузке файла: %s", e)
            await update.message.reply_text(f"Произошла ошибка при загрузке файла: {e}")
            return

        export_keyboard = [
            [
                InlineKeyboardButton(
                    "Выгрузка без передач", callback_data=EXPORT_WITHOUT_TRANSFERS
                )
            ],
            [
                InlineKeyboardButton(
                    "Выгрузка с передачами", callback_data=EXPORT_WITH_TRANSFERS
                )
            ],
            [
                InlineKeyboardButton(
                    "Выгрузка только передач", callback_data=EXPORT_ONLY_TRANSFERS
                )
            ],
            [InlineKeyboardButton("Отменить", callback_data="cancel")],
        ]
        await update.message.reply_text(
            "Файл загружен. Выберите тип выгрузки:",
            reply_markup=InlineKeyboardMarkup(export_keyboard),
        )

    async def process_export_mode(self, query, context: CallbackContext):
        if self.processing_lock.locked():
            await query.message.reply_text(
                "Сейчас выполняется другая выгрузка. Попробуйте через минуту."
            )
            return

        file_path = context.user_data.get("pending_file_path")
        if not file_path or not os.path.exists(file_path):
            await query.message.reply_text(
                "Файл для обработки не найден. Загрузите файл заново."
            )
            return

        mode = query.data
        mode_title = {
            EXPORT_WITHOUT_TRANSFERS: "Выгрузка без передач",
            EXPORT_WITH_TRANSFERS: "Выгрузка с передачами",
            EXPORT_ONLY_TRANSFERS: "Выгрузка только передач",
        }[mode]

        await query.edit_message_reply_markup(reply_markup=None)

        async with self.processing_lock:
            user_message = await query.message.reply_text(f"{mode_title}... [0%]")
            try:
                sh = self.gc.open_by_key(self.spreadsheet_id)
                ws = sh.sheet1
                ws.clear()
                logging.info("Таблица очищена перед загрузкой новых данных.")

                for i in range(5, 101, 5):
                    await asyncio.sleep(0.5)
                    progress = f"[{'#' * (i // 10)}{'-' * (10 - i // 10)}] {i}%"
                    await user_message.edit_text(f"{mode_title}... {progress}")

                rows, unknown_summary = self.process_file(file_path, mode)
                logging.info(
                    "Файл обработан. Режим: %s. Строк для выгрузки: %s",
                    mode_title,
                    len(rows),
                )
                self.upload_to_google_sheets(rows)

                result_message = (
                    f"Загрузка завершена ({mode_title})!\n"
                    f"Данные загружены в таблицу:\n"
                    f"https://docs.google.com/spreadsheets/d/{self.spreadsheet_id}"
                )
                if unknown_summary["count"] > 0:
                    values = ", ".join(unknown_summary["values"])
                    result_message += (
                        "\n\nНестандартные Гофры (включены с учетом порога > 2000): "
                        f"{unknown_summary['count']} шт.\n"
                        f"Значения: {values}"
                    )
                await user_message.edit_text(result_message)
            except ValueError as e:
                logging.error("Ошибка в структуре файла: %s", e)
                await user_message.edit_text(f"Ошибка файла: {e}")
            except Exception as e:
                logging.error("Ошибка при выгрузке: %s", e)
                await user_message.edit_text(f"Произошла ошибка при выгрузке: {e}")
            finally:
                await self.cleanup_pending_file(context)

    def process_file(self, file_path: str, export_mode: str):
        df = pd.read_excel(file_path)
        missing_columns = REQUIRED_COLUMNS - set(df.columns)
        if missing_columns:
            missing = ", ".join(sorted(missing_columns))
            raise ValueError(f"В файле нет обязательных колонок: {missing}")

        df["Стоимость"] = pd.to_numeric(df["Стоимость"], errors="coerce").fillna(0)
        df["ШК"] = df["ШК"].astype(str)
        df["Гофра"] = (
            df["Гофра"].astype(str).str.strip().str.replace(r"\.0$", "", regex=True)
        )

        agg = (
            df.groupby("Гофра", as_index=False)
            .agg(
                **{
                    "Количество ШК": ("ШК", "count"),
                    "Стоимость": ("Стоимость", "sum"),
                    "ШК": ("ШК", lambda s: "\n".join(s.tolist())),
                }
            )
            .sort_values("Стоимость", ascending=False)
        )

        agg = agg[agg["Стоимость"] > 2000]
        transfers_mask = agg["Гофра"].str.startswith(("3", "4"))

        if export_mode == EXPORT_WITHOUT_TRANSFERS:
            agg = agg[~transfers_mask]
        elif export_mode == EXPORT_ONLY_TRANSFERS:
            agg = agg[transfers_mask]

        formatted_rows = [
            [row[0], row[1], row[3], row[2]]
            for row in agg[
                ["Гофра", "ШК", "Стоимость", "Количество ШК"]
            ].values.tolist()
        ]

        unknown_values = (
            agg.loc[~agg["Гофра"].str.startswith(("3", "4", "7", "9", "10")), "Гофра"]
            .dropna()
            .astype(str)
            .sort_values()
            .unique()
            .tolist()
        )
        unknown_summary = {"count": len(unknown_values), "values": unknown_values}
        return formatted_rows, unknown_summary

    def upload_to_google_sheets(self, rows):
        sh = self.gc.open_by_key(self.spreadsheet_id)
        ws = sh.sheet1
        batch_update = []
        for i, row in enumerate(rows, start=1):
            batch_update.append(
                {
                    "range": f"D{i}:H{i}",
                    "values": [[row[0], row[1], row[2], row[3]]],
                }
            )
        if batch_update:
            ws.batch_update(batch_update)
        logging.info(
            "Данные успешно загружены в Google Sheets. Добавлено %s строк.", len(rows)
        )

    async def cleanup_pending_file(self, context: CallbackContext):
        file_path = context.user_data.pop("pending_file_path", None)
        if file_path and os.path.exists(file_path):
            try:
                os.remove(file_path)
                logging.info("Удален временный файл: %s", file_path)
            except Exception as e:
                logging.warning(
                    "Не удалось удалить временный файл %s: %s", file_path, e
                )

    def run(self):
        self.application.run_polling()


if __name__ == "__main__":
    setup_logging()
    bot = TelegramBot(TELEGRAM_BOT_TOKEN, GOOGLE_CREDENTIALS_PATH, SPREADSHEET_ID)
    bot.run()
