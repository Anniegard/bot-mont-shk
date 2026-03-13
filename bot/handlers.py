from __future__ import annotations

import asyncio
import logging
import time
import re
from html import escape
from pathlib import Path

from telegram import (
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    ReplyKeyboardMarkup,
    Update,
)
from telegram.error import BadRequest
from telegram.ext import (
    Application,
    CallbackContext,
    CallbackQueryHandler,
    CommandHandler,
    MessageHandler,
    filters,
)

from bot.config import Config
from bot.services.block_ids import load_block_ids
from bot.services.excel import (
    EXPORT_ONLY_TRANSFERS,
    EXPORT_WITH_TRANSFERS,
    EXPORT_WITHOUT_TRANSFERS,
    process_file,
)
from bot.services.excel_24h import (
    build_24h_table,
    load_snapshot,
    process_24h_file,
    save_snapshot,
)
from bot.services.no_move_map import load_no_move_map, save_no_move_map
from bot.services.file_sources import (
    download_from_url,
    is_url,
    maybe_extract_zip,
)
from bot.services.yadisk import YaDiskError, yadisk_download_file, yadisk_list_latest
from bot.services.yadisk_ingest import (
    SOURCE_KIND_24H,
    SOURCE_KIND_NO_MOVE,
    ingest_yadisk_rows,
)
from bot.services.sheets import update_tables

logger = logging.getLogger(__name__)

BUTTON_NO_MOVE = "📦 Без движения"
BUTTON_24H = "⏱ 24 часа (обновить)"
BUTTON_ADMIN = "🛠 Админ-панель"
BUTTON_YA_LAST = "☁️ Взять с Я.Диска (последний файл)"
BUTTON_YA_HELP = "📎 Инструкция по загрузке на Диск"

EXPECTED_NO_MOVE = "no_move"
EXPECTED_24H = "24h"
NO_MOVE_EXPORT_KEY = "no_move_export_mode"

NO_MOVE_EXPORT_CALLBACK_PREFIX = "no_move_mode:"
NO_MOVE_EXPORT_BUTTONS = {
    f"{NO_MOVE_EXPORT_CALLBACK_PREFIX}with": (
        EXPORT_WITH_TRANSFERS,
        "С передачами",
    ),
    f"{NO_MOVE_EXPORT_CALLBACK_PREFIX}without": (
        EXPORT_WITHOUT_TRANSFERS,
        "Без передач",
    ),
    f"{NO_MOVE_EXPORT_CALLBACK_PREFIX}only": (
        EXPORT_ONLY_TRANSFERS,
        "Только передачи",
    ),
}
NO_MOVE_EXPORT_ORDER = [
    f"{NO_MOVE_EXPORT_CALLBACK_PREFIX}with",
    f"{NO_MOVE_EXPORT_CALLBACK_PREFIX}without",
    f"{NO_MOVE_EXPORT_CALLBACK_PREFIX}only",
]

MAX_TG_UPLOAD_BYTES = 20 * 1024 * 1024
MAX_URL_BYTES = 200 * 1024 * 1024


class BotHandlers:
    def __init__(self, config: Config, gspread_client):
        self.config = config
        self.gc = gspread_client
        self.processing_lock = asyncio.Lock()
        self.last_no_move_product_ids = set()

        root_dir = Path(__file__).resolve().parent.parent
        self.workdir = root_dir
        self.data_dir = root_dir / "data"
        self.block_ids_path = self.data_dir / "block_ids.txt"
        self.snapshot_path = self.data_dir / "last_24h_snapshot.json"
        self.snapshot_meta_path = self.data_dir / "last_24h_meta.json"
        self.no_move_map_path = self.data_dir

        self.reply_keyboard = ReplyKeyboardMarkup(
            [
                [BUTTON_NO_MOVE],
                [BUTTON_24H],
                [BUTTON_YA_LAST],
                [BUTTON_YA_HELP],
                [BUTTON_ADMIN],
            ],
            resize_keyboard=True,
        )

        logger.debug(
            "YANDEX token prefix: %s; dirs: no_move=%s h24=%s",
            (
                (self.config.yandex_oauth_token[:8] + "***")
                if self.config.yandex_oauth_token
                else "none"
            ),
            self.config.yandex_no_move_dir,
            self.config.yandex_24h_dir,
        )

    def register(self, application: Application) -> None:
        application.add_handler(CommandHandler("start", self.start))
        application.add_handler(CommandHandler("admin", self.admin))
        application.add_handler(
            MessageHandler(
                filters.Regex(f"^{re.escape(BUTTON_NO_MOVE)}$"), self.select_no_move
            )
        )
        application.add_handler(
            MessageHandler(filters.Regex(f"^{re.escape(BUTTON_24H)}$"), self.select_24h)
        )
        application.add_handler(
            MessageHandler(
                filters.Regex(f"^{re.escape(BUTTON_YA_LAST)}$"),
                self.handle_yadisk_latest,
            )
        )
        application.add_handler(
            MessageHandler(
                filters.Regex(f"^{re.escape(BUTTON_YA_HELP)}$"), self.handle_yadisk_help
            )
        )
        application.add_handler(
            MessageHandler(filters.Regex(f"^{re.escape(BUTTON_ADMIN)}$"), self.admin)
        )
        application.add_handler(MessageHandler(filters.Document.ALL, self.handle_file))
        application.add_handler(
            MessageHandler(filters.TEXT & ~filters.COMMAND, self.handle_text)
        )
        application.add_handler(
            CallbackQueryHandler(
                self.no_move_mode_selected, pattern=f"^{NO_MOVE_EXPORT_CALLBACK_PREFIX}"
            )
        )
        application.add_handler(CallbackQueryHandler(self.admin_button_handler))

    async def start(self, update: Update, context: CallbackContext) -> None:
        user = update.effective_user
        context.user_data["expected_upload"] = None
        message = (
            "Привет! Я могу обработать Excel:\n"
            "• 📦 Без движения — основной файл с гофрой/идентификаторами.\n"
            "• ⏱ 24 часа — файл прогноза списаний, обновляет правую таблицу.\n"
            "Выберите режим кнопкой снизу и пришлите файл (.xlsx) до 20 МБ или ссылку на файл (Яндекс.Диск/прямая).\n"
            "Между строками выгрузки будет пустая строка для удобного CTRL+A."
        )
        logger.info("Команда /start user_id=%s username=%s", user.id, user.username)
        await update.message.reply_text(message, reply_markup=self.reply_keyboard)

    async def admin(self, update: Update, context: CallbackContext) -> None:
        user = update.effective_user
        if not self._is_admin(user.id):
            logger.warning(
                "Попытка доступа к /admin user_id=%s username=%s",
                user.id,
                user.username,
            )
            await update.message.reply_text(
                "У вас нет прав для использования этой команды.",
                reply_markup=self.reply_keyboard,
            )
            return

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
            "Выберите действие:",
            reply_markup=InlineKeyboardMarkup(admin_keyboard),
        )

    async def select_no_move(self, update: Update, context: CallbackContext) -> None:
        context.user_data["expected_upload"] = EXPECTED_NO_MOVE
        context.user_data[NO_MOVE_EXPORT_KEY] = None
        user = update.effective_user
        logger.info(
            "Выбран режим без движения user_id=%s username=%s", user.id, user.username
        )
        await update.message.reply_text(
            "Выберите тип выгрузки для режима «Без движения»:",
            reply_markup=self._no_move_mode_keyboard(),
        )

    async def select_24h(self, update: Update, context: CallbackContext) -> None:
        context.user_data["expected_upload"] = EXPECTED_24H
        user = update.effective_user
        logger.info("Выбран режим 24ч user_id=%s username=%s", user.id, user.username)
        await update.message.reply_text(
            "Ок, пришли Excel «24 часа» (документ до 20 МБ) или ссылку на файл.",
            reply_markup=self.reply_keyboard,
        )

    def _no_move_mode_keyboard(self) -> InlineKeyboardMarkup:
        buttons = [
            InlineKeyboardButton(NO_MOVE_EXPORT_BUTTONS[cb][1], callback_data=cb)
            for cb in NO_MOVE_EXPORT_ORDER
        ]
        return InlineKeyboardMarkup([buttons])

    async def no_move_mode_selected(
        self, update: Update, context: CallbackContext
    ) -> None:
        query = update.callback_query
        await query.answer()
        data = query.data
        if data not in NO_MOVE_EXPORT_BUTTONS:
            return

        export_mode, label = NO_MOVE_EXPORT_BUTTONS[data]
        context.user_data[NO_MOVE_EXPORT_KEY] = export_mode
        context.user_data["expected_upload"] = EXPECTED_NO_MOVE
        user = query.from_user
        logger.info(
            "Выбран подрежим без движения user_id=%s username=%s mode=%s",
            user.id,
            user.username,
            export_mode,
        )

        try:
            await query.edit_message_reply_markup(reply_markup=None)
        except Exception:
            pass

        await query.message.reply_text(
            f"Режим выбран: {label.lower()}. Пришлите Excel или ссылку.",
            reply_markup=self.reply_keyboard,
        )

    async def _ensure_no_move_mode_selected(
        self, update: Update, context: CallbackContext
    ) -> str | None:
        export_mode = context.user_data.get(NO_MOVE_EXPORT_KEY)
        if export_mode:
            return export_mode
        await update.message.reply_text(
            "Сначала выберите тип выгрузки для режима «Без движения»:",
            reply_markup=self._no_move_mode_keyboard(),
        )
        return None

    def _sheet_link_html(self) -> str | None:
        if not self.config.spreadsheet_id:
            return None
        link = (
            f"https://docs.google.com/spreadsheets/d/{self.config.spreadsheet_id}/edit"
        )
        return f'<a href="{escape(link)}">Ссылка на таблицу</a>'

    async def handle_yadisk_help(
        self, update: Update, context: CallbackContext
    ) -> None:
        text = (
            "Как загрузить файл на Яндекс.Диск и отправить боту:\n"
            f"- Для «без движения» положите файл в папку {self.config.yandex_no_move_dir or '/BOT_UPLOADS/no_move/'}\n"
            f"- Для «24 часа» — в папку {self.config.yandex_24h_dir or '/BOT_UPLOADS/24h/'}\n"
            "Бот возьмёт последний Excel/zip из выбранной папки.\n"
            "Выберите режим, затем нажмите «☁️ Взять с Я.Диска (последний файл)»."
        )
        await update.message.reply_text(text, reply_markup=self.reply_keyboard)

    async def handle_yadisk_latest(
        self, update: Update, context: CallbackContext
    ) -> None:
        user = update.effective_user
        expected = context.user_data.get("expected_upload")
        if not expected:
            await update.message.reply_text(
                "Сначала выберите режим: 📦 Без движения или ⏱ 24 часа (обновить).",
                reply_markup=self.reply_keyboard,
            )
            return

        if expected == EXPECTED_NO_MOVE:
            export_mode = await self._ensure_no_move_mode_selected(update, context)
            if not export_mode:
                return

        if not self.config.yandex_oauth_token:
            await update.message.reply_text(
                "Яндекс OAuth токен не настроен. Обратитесь к администратору."
            )
            return

        folder = (
            self.config.yandex_no_move_dir
            if expected == EXPECTED_NO_MOVE
            else self.config.yandex_24h_dir
        ) or "/"
        await update.message.reply_text("Ищу последний файл на Я.Диске...")

        try:
            latest = await yadisk_list_latest(
                self.config.yandex_oauth_token,
                folder,
                self.config.yandex_allowed_exts,
            )
            name = latest.get("name")
            path = latest.get("path")
            modified = latest.get("modified")
            size = latest.get("size")

            await update.message.reply_text(f"Найден файл: {name}\nСкачиваю…")

            filename_suffix = Path(name).suffix or ".xlsx"
            temp_path = (
                self.workdir / f"yadisk_{user.id}_{int(time.time())}{filename_suffix}"
            )
            cleanup_paths = [temp_path]

            download_start = time.perf_counter()
            result = await yadisk_download_file(
                self.config.yandex_oauth_token,
                path,
                str(temp_path),
                max_bytes=self.config.yandex_max_mb * 1024 * 1024,
            )
            download_duration = time.perf_counter() - download_start

            excel_path = maybe_extract_zip(result["path"], self.workdir)
            if excel_path != result["path"]:
                cleanup_paths.append(excel_path)

            file_info = {
                "filename": name,
                "size": size or result.get("size"),
                "source": "yadisk",
                "modified": modified,
                "source_path": f"{path}|modified:{modified}" if modified else path,
            }

            await update.message.reply_text("Файл скачан, обрабатываю…")
            await self._process_excel_file(
                expected, update, context, excel_path, file_info
            )

            logger.info(
                "YaDisk файл обработан: user_id=%s username=%s mode=%s name=%s size=%s modified=%s duration=%.3fs",
                user.id,
                user.username,
                expected,
                name,
                size,
                modified,
                download_duration,
            )
        except YaDiskError as exc:
            logger.warning("Ошибка Яндекс.Диск user_id=%s: %s", user.id, exc)
            await update.message.reply_text(f"Ошибка Яндекс.Диска: {exc}")
        except Exception:
            logger.exception("Сбой при загрузке с Я.Диска user_id=%s", user.id)
            await update.message.reply_text("Не удалось получить файл с Я.Диска.")
        finally:
            for p in locals().get("cleanup_paths", []):
                try:
                    Path(p).unlink(missing_ok=True)
                except Exception:
                    logger.warning("Не удалось удалить временный файл %s", p)

    async def handle_text(self, update: Update, context: CallbackContext) -> None:
        text = (update.message.text or "").strip()
        if is_url(text):
            expected = context.user_data.get("expected_upload")
            if not expected:
                await update.message.reply_text(
                    "Сначала выберите режим кнопкой снизу, затем пришлите ссылку.",
                    reply_markup=self.reply_keyboard,
                )
                return
            if expected == EXPECTED_NO_MOVE:
                export_mode = await self._ensure_no_move_mode_selected(
                    update, context
                )
                if not export_mode:
                    return
            await self._process_url_file(update, context, text, expected)
            return

        await update.message.reply_text(
            "Выберите режим кнопкой снизу и пришлите Excel (.xlsx до 20 МБ) или ссылку на файл.",
            reply_markup=self.reply_keyboard,
        )

    async def handle_file(self, update: Update, context: CallbackContext) -> None:
        user = update.effective_user
        document = update.message.document
        expected = context.user_data.get("expected_upload")

        if not expected:
            await update.message.reply_text(
                "Сначала выберите режим: 📦 Без движения или ⏱ 24 часа (обновить).",
                reply_markup=self.reply_keyboard,
            )
            return

        if expected == EXPECTED_NO_MOVE:
            export_mode = await self._ensure_no_move_mode_selected(update, context)
            if not export_mode:
                return

        if self.processing_lock.locked():
            await update.message.reply_text(
                "Сейчас выполняется другая обработка. Повторите чуть позже."
            )
            return

        try:
            if document.file_size and document.file_size > MAX_TG_UPLOAD_BYTES:
                logger.warning(
                    "Файл слишком большой (document) user_id=%s mode=%s filename=%s size=%s",
                    user.id,
                    expected,
                    document.file_name,
                    document.file_size,
                )
                await self.send_big_file_instructions(update)
                return

            try:
                tg_file = await document.get_file()
            except BadRequest as exc:
                if "File is too big" in str(exc):
                    await self.send_big_file_instructions(update)
                    return
                logger.exception(
                    "Ошибка get_file user_id=%s username=%s", user.id, user.username
                )
                await update.message.reply_text("Не удалось скачать файл из Telegram.")
                return

            original_name = document.file_name or "file.xlsx"
            filename = f"upload_{user.id}_{int(time.time())}{Path(original_name).suffix or '.xlsx'}"
            target_path = self.workdir / filename
            file_path = await tg_file.download_to_drive(custom_path=str(target_path))

            cleanup_paths = [file_path]
            try:
                excel_path = maybe_extract_zip(file_path, self.workdir)
                if excel_path != file_path:
                    cleanup_paths.append(excel_path)

                if not excel_path.lower().endswith((".xlsx", ".xls")):
                    await update.message.reply_text(
                        "Ожидался Excel (.xlsx/.xls). Проверьте файл."
                    )
                    return

                file_info = {
                    "filename": original_name,
                    "size": document.file_size,
                    "source": "telegram_document",
                    "source_path": f"telegram:{document.file_unique_id or document.file_id}",
                }
                await self._process_excel_file(
                    expected, update, context, excel_path, file_info
                )
            finally:
                for p in cleanup_paths:
                    try:
                        Path(p).unlink(missing_ok=True)
                    except Exception:
                        logger.warning("Не удалось удалить временный файл %s", p)
        except Exception:
            logger.exception(
                "Ошибка загрузки файла user_id=%s username=%s", user.id, user.username
            )
            await update.message.reply_text(
                "Ошибка при загрузке файла. Попробуйте еще раз."
            )

    async def _process_url_file(
        self, update: Update, context: CallbackContext, url: str, expected: str
    ) -> None:
        user = update.effective_user
        status_message = await update.message.reply_text("Скачиваю файл по ссылке...")
        filename_suffix = Path(url).suffix or ".xlsx"
        temp_path = self.workdir / f"url_{user.id}_{int(time.time())}{filename_suffix}"
        cleanup_paths = [temp_path]
        try:
            download_start = time.perf_counter()
            file_path, size, source_type = await download_from_url(
                url, str(temp_path), max_bytes=MAX_URL_BYTES
            )
            download_duration = time.perf_counter() - download_start

            excel_path = maybe_extract_zip(file_path, self.workdir)
            if excel_path != file_path:
                cleanup_paths.append(excel_path)

            if not excel_path.lower().endswith((".xlsx", ".xls")):
                await status_message.edit_text(
                    "Ожидался Excel (.xlsx/.xls). Проверьте файл."
                )
                return

            file_info = {
                "filename": Path(excel_path).name,
                "size": size,
                "source": f"{source_type}_link",
                "source_path": url,
                "source_url": url,
            }
            logger.info(
                "Файл по ссылке скачан user_id=%s source=%s url=%s size=%s duration=%.3fs mode=%s",
                user.id,
                source_type,
                url,
                size,
                download_duration,
                expected,
            )
            await status_message.edit_text("Файл скачан, начинаю обработку...")
            await self._process_excel_file(
                expected, update, context, excel_path, file_info
            )
        except ValueError as exc:
            logger.warning(
                "Ошибка скачивания по ссылке user_id=%s url=%s: %s", user.id, url, exc
            )
            await status_message.edit_text(str(exc))
        except Exception:
            logger.exception(
                "Ошибка скачивания по ссылке user_id=%s url=%s", user.id, url
            )
            await status_message.edit_text("Не удалось скачать файл по ссылке.")
        finally:
            for p in cleanup_paths:
                try:
                    Path(p).unlink(missing_ok=True)
                except Exception:
                    logger.warning("Не удалось удалить временный файл %s", p)

    async def _process_excel_file(
        self,
        expected: str,
        update: Update,
        context: CallbackContext,
        file_path: str,
        file_info: dict,
    ) -> None:
        if expected == EXPECTED_NO_MOVE:
            export_mode = context.user_data.get(NO_MOVE_EXPORT_KEY)
            if not export_mode:
                await update.message.reply_text(
                    "Сначала выберите тип выгрузки для режима «Без движения»:",
                    reply_markup=self._no_move_mode_keyboard(),
                )
                return
            await self._handle_no_move_file(
                update, context, file_path, file_info, export_mode
            )
        elif expected == EXPECTED_24H:
            await self._handle_24h_file(update, context, file_path, file_info)
        else:
            await update.message.reply_text("Неизвестный режим. Выберите кнопку снизу.")

    def _run_raw_yadisk_ingest(
        self,
        *,
        file_path: str,
        file_info: dict,
        source_kind: str,
    ) -> dict | None:
        try:
            summary = ingest_yadisk_rows(
                file_path=file_path,
                source_kind=source_kind,
                file_info=file_info,
                db_path=self.config.db_path,
            )
            logger.info(
                "Raw ingest complete: kind=%s source=%s path=%s rows_read=%s rows_written=%s rows_linked=%s import_id=%s",
                source_kind,
                file_info.get("source"),
                summary.get("source_path"),
                summary.get("rows_read"),
                summary.get("rows_written"),
                summary.get("rows_linked"),
                summary.get("import_id"),
            )
            return summary
        except Exception:
            logger.exception(
                "Raw ingest failed: kind=%s source=%s filename=%s",
                source_kind,
                file_info.get("source"),
                file_info.get("filename"),
            )
            return None

    async def _handle_no_move_file(
        self,
        update: Update,
        context: CallbackContext,
        file_path: str,
        file_info: dict,
        export_mode: str,
    ) -> None:
        user = update.effective_user
        context.user_data["expected_upload"] = None
        status_message = await update.message.reply_text("Читаю файл «без движения»...")

        async with self.processing_lock:
            start_ts = time.perf_counter()
            try:
                rows, unknown_summary, stats = process_file(file_path, export_mode)
                self._run_raw_yadisk_ingest(
                    file_path=file_path,
                    file_info=file_info,
                    source_kind=SOURCE_KIND_NO_MOVE,
                )

                processing_duration = time.perf_counter() - start_ts
                product_ids = stats.get("product_ids", set())
                self.last_no_move_product_ids = set(product_ids)
                id_to_tary = stats.get("id_to_tary", {})

                # save mapping for fallback
                save_no_move_map(
                    id_to_tary,
                    {
                        "uploaded_at": time.strftime("%Y-%m-%dT%H:%M:%S"),
                        "source_filename": file_info.get("filename"),
                        "identifiers_count": len(id_to_tary),
                        "groups_count": len(rows),
                    },
                    self.no_move_map_path,
                )

                # Load 24h snapshot if exists
                snapshot, meta = load_snapshot(
                    self.snapshot_path, self.snapshot_meta_path
                )
                right_rows = build_24h_table(snapshot, id_to_tary) if snapshot else []

                update_tables(
                    self.gc,
                    self.config.spreadsheet_id,
                    self.config.worksheet_name,
                    rows,
                    right_rows,
                    meta,
                    skip_left=False,
                    skip_right=False,
                )

                export_label = next(
                    (label for mode, label in NO_MOVE_EXPORT_BUTTONS.values() if mode == export_mode),
                    export_mode,
                )
                safe_label = escape(export_label.lower())
                result_message = (
                    "Готово. Левая таблица обновлена."
                    f"\nТип выгрузки: {safe_label}."
                    f"\nСтрок для выгрузки: {len(rows)}."
                    f"\nСтрок 24ч: {len(right_rows)}."
                )
                if unknown_summary["count"] > 0:
                    values = ", ".join(unknown_summary["values"])
                    result_message += (
                        f"\nНестандартные Гофры (порог > 2000): {unknown_summary['count']} шт."
                        f"\nЗначения: {escape(values)}"
                    )
                link_html = self._sheet_link_html()
                if link_html:
                    result_message += f"\n{link_html}"
                await status_message.edit_text(
                    result_message,
                    parse_mode="HTML",
                    disable_web_page_preview=True,
                )

                logger.info(
                    "Без движения успешно: user_id=%s username=%s file=%s size=%s source=%s rows=%s duration=%.3fs products=%s right_rows=%s export_mode=%s",
                    user.id,
                    user.username,
                    file_info.get("filename"),
                    file_info.get("size"),
                    file_info.get("source"),
                    len(rows),
                    processing_duration,
                    len(product_ids),
                    len(right_rows),
                    export_mode,
                )
            except ValueError as exc:
                logger.warning("Ошибка файла без движения user_id=%s: %s", user.id, exc)
                await status_message.edit_text(f"Ошибка файла: {exc}")
            except Exception:
                logger.exception(
                    "Ошибка при обработке без движения user_id=%s", user.id
                )
                await status_message.edit_text(
                    "Произошла ошибка при обработке. Подробности в логах."
                )

    async def _handle_24h_file(
        self, update: Update, context: CallbackContext, file_path: str, file_info: dict
    ) -> None:
        user = update.effective_user
        context.user_data["expected_upload"] = None
        status_message = await update.message.reply_text("Читаю файл «24 часа»...")

        block_ids = load_block_ids(self.block_ids_path)

        async with self.processing_lock:
            start_ts = time.perf_counter()
            try:
                snapshot, meta = process_24h_file(file_path, block_ids)
                self._run_raw_yadisk_ingest(
                    file_path=file_path,
                    file_info=file_info,
                    source_kind=SOURCE_KIND_24H,
                )
                save_snapshot(
                    snapshot, meta, self.snapshot_path, self.snapshot_meta_path
                )
                duration = time.perf_counter() - start_ts

                # Построить правую таблицу: пересечение с последним no_move, иначе всё
                # mapping priority: current no_move ids -> saved map
                id_to_tary = None
                if self.last_no_move_product_ids:
                    # try load latest map from disk to include taras
                    id_to_tary, _ = load_no_move_map(self.no_move_map_path)
                if not id_to_tary:
                    id_to_tary, _ = load_no_move_map(self.no_move_map_path)
                if not id_to_tary:
                    await status_message.edit_text(
                        "Файл 24ч обновлён, но нет сохранённой карты ID тары. Сначала выполните выгрузку «Без движения»."
                    )
                    return

                right_rows = build_24h_table(snapshot, id_to_tary)

                update_tables(
                    self.gc,
                    self.config.spreadsheet_id,
                    self.config.worksheet_name,
                    left_rows=[],
                    right_rows=right_rows,
                    right_meta=meta.__dict__,
                    skip_left=True,
                    skip_right=False,
                )

                result_message = (
                    f"Файл 24ч обновлён и загружен в таблицу.\nСтрок в исходнике: {meta.rows_total}."
                    f"\nПосле фильтров: {meta.rows_after_filter}."
                    f"\nСохранено уникальных товаров: {meta.rows_valid}."
                    f"\nСтрок выгружено в правый блок: {len(right_rows)}."
                )
                link_html = self._sheet_link_html()
                if link_html:
                    result_message += f"\n{link_html}"
                await status_message.edit_text(
                    result_message,
                    parse_mode="HTML",
                    disable_web_page_preview=True,
                )

                logger.info(
                    "24ч обновлено: user_id=%s username=%s file=%s size=%s source=%s rows_total=%s rows_after_filter=%s rows_valid=%s "
                    "dropped_missing=%s dropped_forecast=%s dropped_block=%s duration=%.3fs block_ids=%s right_rows=%s",
                    user.id,
                    user.username,
                    file_info.get("filename"),
                    file_info.get("size"),
                    file_info.get("source"),
                    meta.rows_total,
                    meta.rows_after_filter,
                    meta.rows_valid,
                    meta.dropped_missing,
                    meta.dropped_forecast,
                    meta.dropped_block,
                    duration,
                    len(block_ids),
                    len(right_rows),
                )
            except ValueError as exc:
                logger.warning("Ошибка файла 24ч user_id=%s: %s", user.id, exc)
                await status_message.edit_text(f"Ошибка файла: {exc}")
            except Exception:
                logger.exception("Ошибка при обработке 24ч user_id=%s", user.id)
                await status_message.edit_text(
                    "Произошла ошибка при обработке 24ч. Подробности в логах."
                )

    async def admin_button_handler(
        self, update: Update, context: CallbackContext
    ) -> None:
        query = update.callback_query
        await query.answer()
        data = query.data
        user = query.from_user

        if data == "view_logs":
            if not self._is_admin(user.id):
                logger.warning(
                    "Отказ в доступе view_logs user_id=%s username=%s",
                    user.id,
                    user.username,
                )
                await query.message.reply_text(
                    "У вас нет прав для выполнения этого действия."
                )
                return
            await self._send_logs(query)
            return

        if data == "start_task":
            if not self._is_admin(user.id):
                await query.message.reply_text(
                    "У вас нет прав для выполнения этого действия."
                )
                return
            await query.message.reply_text(
                "Выберите режим кнопкой снизу и отправьте файл.",
                reply_markup=self.reply_keyboard,
            )
            return

        if data == "stop_bot":
            if not self._is_admin(user.id):
                await query.message.reply_text(
                    "У вас нет прав для выполнения этого действия."
                )
                return
            logger.info("Бот остановлен админом user_id=%s", user.id)
            await query.message.reply_text("Останавливаю бота...")
            await context.application.stop()

    async def send_big_file_instructions(self, update: Update) -> None:
        text = (
            "Файл слишком большой для скачивания через Telegram или недоступен.\n"
            "Как отправить ссылку (Яндекс.Диск):\n"
            "1) Загрузите файл на Яндекс.Диск\n"
            "2) Нажмите «Поделиться» и включите доступ по ссылке\n"
            "3) Скопируйте ссылку вида https://disk.yandex.ru/d/...\n"
            "4) Пришлите эту ссылку сюда (после выбора режима 📦 или ⏱).\n"
            "Бот скачает файл по ссылке и обработает."
        )
        await update.message.reply_text(text, reply_markup=self.reply_keyboard)

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
            from io import BytesIO

            buffer = BytesIO(text.encode("utf-8"))
            buffer.name = "bot_tail.log"
            await query.message.reply_document(
                document=buffer, caption="Последние 200 строк логов"
            )

    def _is_admin(self, user_id: int) -> bool:
        return bool(self.config.admin_user_id) and str(user_id) == str(
            self.config.admin_user_id
        )
