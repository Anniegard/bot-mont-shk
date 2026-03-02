from __future__ import annotations

import asyncio
import logging
import time
from io import BytesIO
from pathlib import Path
from typing import Optional

from telegram import InlineKeyboardButton, InlineKeyboardMarkup, Update
from telegram.ext import (
    Application,
    CallbackContext,
    CallbackQueryHandler,
    CommandHandler,
    MessageHandler,
    filters,
)

from bot.config import Config
from bot.services.excel import (
    EXPORT_ONLY_TRANSFERS,
    EXPORT_WITHOUT_TRANSFERS,
    EXPORT_WITH_TRANSFERS,
    process_file,
)
from bot.services.sheets import upload_to_google_sheets

# Hard limits
MAX_UPLOAD_SIZE_BYTES = 20 * 1024 * 1024

logger = logging.getLogger(__name__)


EXPORT_MODE_TITLES = {
    EXPORT_WITHOUT_TRANSFERS: "Выгрузка без передач",
    EXPORT_WITH_TRANSFERS: "Выгрузка с передачами",
    EXPORT_ONLY_TRANSFERS: "Выгрузка только передач",
}


class BotHandlers:
    def __init__(self, config: Config, gspread_client):
        self.config = config
        self.gc = gspread_client
        self.processing_lock = asyncio.Lock()

    def register(self, application: Application) -> None:
        application.add_handler(CommandHandler("start", self.start))
        application.add_handler(CommandHandler("admin", self.admin))
        application.add_handler(
            MessageHandler(
                filters.Document.MimeType(
                    "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
                ),
                self.handle_file,
            )
        )
        application.add_handler(CallbackQueryHandler(self.admin_button_handler))

    async def start(self, update: Update, context: CallbackContext) -> None:
        user = update.effective_user
        logger.info("Команда /start от user_id=%s username=%s", user.id, user.username)
        await update.message.reply_text(
            "Привет! Отправь мне Excel (.xlsx), я обработаю и загружу данные в Google Sheets."
        )

    async def admin(self, update: Update, context: CallbackContext) -> None:
        user = update.effective_user
        if not self._is_admin(user.id):
            logger.warning("Попытка доступа к /admin от user_id=%s username=%s", user.id, user.username)
            await update.message.reply_text("У вас нет прав для использования этой команды.")
            return

        admin_keyboard = [
            [InlineKeyboardButton("Просмотреть логи", callback_data="view_logs")],
            [InlineKeyboardButton("Запустить новую задачу", callback_data="start_task")],
            [InlineKeyboardButton("Остановить бота", callback_data="stop_bot")],
        ]
        await update.message.reply_text("Выберите действие:", reply_markup=InlineKeyboardMarkup(admin_keyboard))

    async def handle_file(self, update: Update, context: CallbackContext) -> None:
        user = update.effective_user
        document = update.message.document
        logger.info(
            "Получен файл от user_id=%s username=%s filename=%s size=%s",
            user.id,
            user.username,
            document.file_name,
            document.file_size,
        )

        if self.processing_lock.locked():
            await update.message.reply_text("Сейчас выполняется другая выгрузка. Повторите чуть позже.")
            return

        if document.file_size and document.file_size > MAX_UPLOAD_SIZE_BYTES:
            await update.message.reply_text(
                f"Файл слишком большой. Максимум: {MAX_UPLOAD_SIZE_BYTES // (1024 * 1024)} МБ."
            )
            return

        await self._cleanup_pending_file(context)

        try:
            tg_file = await document.get_file()
            filename = f"uploaded_{user.id}_{int(time.time())}.xlsx"
            target_path = Path(context.application.bot_data.get("workdir", Path.cwd())) / filename
            file_path = await tg_file.download_to_drive(custom_path=str(target_path))
            context.user_data["pending_file_path"] = file_path
            context.user_data["pending_file_info"] = {
                "filename": document.file_name or filename,
                "size": document.file_size,
                "user_id": user.id,
                "username": user.username,
            }
            await update.message.reply_text(
                "Файл загружен. Выберите тип выгрузки:",
                reply_markup=self._export_keyboard(),
            )
        except Exception as exc:  # pragma: no cover - runtime safety
            logger.exception("Ошибка при загрузке файла от user_id=%s: %s", user.id, exc)
            await update.message.reply_text("Произошла ошибка при загрузке файла. Попробуйте ещё раз.")

    async def admin_button_handler(self, update: Update, context: CallbackContext) -> None:
        query = update.callback_query
        await query.answer()
        data = query.data
        user = query.from_user

        if data in EXPORT_MODE_TITLES:
            await self._process_export_mode(query, context, data)
            return

        if data == "cancel":
            await self._cleanup_pending_file(context)
            await query.message.reply_text("Загрузка отменена.")
            return

        if data in {"view_logs", "start_task", "stop_bot"} and not self._is_admin(user.id):
            logger.warning("Отказано в админ-доступе callback=%s user_id=%s username=%s", data, user.id, user.username)
            await query.message.reply_text("У вас нет прав для выполнения этого действия.")
            return

        if data == "view_logs":
            await self._send_logs(query)
            return

        if data == "start_task":
            await query.message.reply_text("Загрузите файл и выберите режим выгрузки.")
            return

        if data == "stop_bot":
            logger.info("Бот остановлен по запросу администратора user_id=%s", user.id)
            await query.message.reply_text("Останавливаю бота...")
            await context.application.stop()

    async def _process_export_mode(self, query, context: CallbackContext, mode: str) -> None:
        user = query.from_user
        if self.processing_lock.locked():
            await query.message.reply_text("Сейчас выполняется другая выгрузка. Попробуйте через минуту.")
            return

        file_path = context.user_data.get("pending_file_path")
        if not file_path or not Path(file_path).exists():
            await query.message.reply_text("Файл для обработки не найден. Загрузите файл заново.")
            return

        await query.edit_message_reply_markup(reply_markup=None)
        status_message = await query.message.reply_text("Файл получен, читаю...")

        async with self.processing_lock:
            total_start = time.perf_counter()
            try:
                process_start = time.perf_counter()
                rows, unknown_summary, stats = process_file(file_path, mode)
                processing_duration = time.perf_counter() - process_start

                await status_message.edit_text("Файл обработан, загружаю в Google Sheets...")
                upload_start = time.perf_counter()
                upload_to_google_sheets(
                    rows,
                    self.gc,
                    self.config.spreadsheet_id,
                    self.config.worksheet_name,
                )
                upload_duration = time.perf_counter() - upload_start
                total_duration = time.perf_counter() - total_start

                result_message = (
                    f"Готово ({EXPORT_MODE_TITLES[mode]}).\n"
                    f"Загружено строк: {len(rows)}.\n"
                    f"Ссылка на таблицу:\nhttps://docs.google.com/spreadsheets/d/{self.config.spreadsheet_id}"
                )
                if unknown_summary["count"] > 0:
                    values = ", ".join(unknown_summary["values"])
                    result_message += (
                        "\n\nНестандартные Гофры (порог > 2000): "
                        f"{unknown_summary['count']} шт.\n"
                        f"Значения: {values}"
                    )
                await status_message.edit_text(result_message)

                file_info = context.user_data.get("pending_file_info", {})
                logger.info(
                    "Успешная загрузка: user_id=%s username=%s mode=%s rows=%s source_rows=%s duration=%.3fs "
                    "processing=%.3fs upload=%.3fs file=%s size=%s unknown=%s",
                    user.id,
                    user.username,
                    mode,
                    len(rows),
                    stats.get("source_rows"),
                    total_duration,
                    processing_duration,
                    upload_duration,
                    file_info.get("filename"),
                    file_info.get("size"),
                    unknown_summary["count"],
                )
            except ValueError as exc:
                logger.warning(
                    "Ошибка валидации файла user_id=%s username=%s: %s", user.id, user.username, exc
                )
                await status_message.edit_text(f"Ошибка файла: {exc}")
            except Exception as exc:  # pragma: no cover - runtime safety
                logger.exception("Ошибка при выгрузке user_id=%s username=%s", user.id, user.username)
                await status_message.edit_text("Произошла ошибка при выгрузке. Проверьте логи.")
            finally:
                await self._cleanup_pending_file(context)

    def _export_keyboard(self) -> InlineKeyboardMarkup:
        return InlineKeyboardMarkup(
            [
                [InlineKeyboardButton(EXPORT_MODE_TITLES[EXPORT_WITHOUT_TRANSFERS], callback_data=EXPORT_WITHOUT_TRANSFERS)],
                [InlineKeyboardButton(EXPORT_MODE_TITLES[EXPORT_WITH_TRANSFERS], callback_data=EXPORT_WITH_TRANSFERS)],
                [InlineKeyboardButton(EXPORT_MODE_TITLES[EXPORT_ONLY_TRANSFERS], callback_data=EXPORT_ONLY_TRANSFERS)],
                [InlineKeyboardButton("Отменить", callback_data="cancel")],
            ]
        )

    async def _cleanup_pending_file(self, context: CallbackContext) -> None:
        file_path = context.user_data.pop("pending_file_path", None)
        context.user_data.pop("pending_file_info", None)
        if file_path:
            path_obj = Path(file_path)
            if path_obj.exists():
                try:
                    path_obj.unlink()
                    logger.info("Удален временный файл: %s", path_obj)
                except Exception as exc:  # pragma: no cover
                    logger.warning("Не удалось удалить временный файл %s: %s", path_obj, exc)

    async def _send_logs(self, query) -> None:
        log_path = Path(__file__).resolve().parent.parent / "logs" / "bot.log"
        if not log_path.exists():
            await query.message.reply_text("Файл логов пока не создан.")
            return

        lines = log_path.read_text(encoding="utf-8").splitlines()
        tail_lines = lines[-200:] if lines else ["(логи пусты)"]
        text = "\n".join(tail_lines)
        if len(text) <= 3500:
            await query.message.reply_text(f"Последние логи (до 200 строк):\n{text}")
        else:
            buffer = BytesIO(text.encode("utf-8"))
            buffer.name = "bot_tail.log"
            await query.message.reply_document(document=buffer, caption="Последние 200 строк логов")

    def _is_admin(self, user_id: int) -> bool:
        return bool(self.config.admin_user_id) and str(user_id) == str(self.config.admin_user_id)
