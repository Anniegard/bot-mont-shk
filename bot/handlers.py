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
from bot.services.raw_review import (
    get_raw_row_details,
    ignore_raw_row,
    list_raw_row_candidates,
    list_unresolved_raw_rows,
    manual_link_raw_row,
    manual_unlink_raw_row,
    mark_raw_row_pending,
)
from bot.services.search_service import (
    DEFAULT_CASE_LIMIT,
    DEFAULT_CASE_RAW_LIMIT,
    DEFAULT_RAW_LIMIT,
    get_case_by_case_id as get_search_case_by_case_id,
    get_raw_rows_for_case,
    search_cases,
    search_raw_rows,
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
        application.add_handler(CommandHandler("case_help", self.case_help))
        application.add_handler(CommandHandler("case", self.case_search))
        application.add_handler(CommandHandler("case_raw", self.case_raw))
        application.add_handler(CommandHandler("raw_find", self.raw_find))
        application.add_handler(CommandHandler("raw_help", self.raw_help))
        application.add_handler(CommandHandler("raw_queue", self.raw_queue))
        application.add_handler(CommandHandler("raw_show", self.raw_show))
        application.add_handler(CommandHandler("raw_candidates", self.raw_candidates))
        application.add_handler(CommandHandler("raw_link", self.raw_link))
        application.add_handler(CommandHandler("raw_unlink", self.raw_unlink))
        application.add_handler(CommandHandler("raw_ignore", self.raw_ignore))
        application.add_handler(CommandHandler("raw_pending", self.raw_pending))
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
        if not await self._ensure_admin_access(update, command_name="/admin"):
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
            "Выберите действие.\nДля разбора raw-строк: /raw_help",
            reply_markup=InlineKeyboardMarkup(admin_keyboard),
        )

    async def raw_help(self, update: Update, context: CallbackContext) -> None:
        if not await self._ensure_admin_access(update, command_name="/raw_help"):
            return
        await update.message.reply_text(
            "Команды разбора raw-строк:\n"
            "/case_help - поиск кейсов и связанных raw\n"
            "/raw_queue [limit] [source_kind] - очередь неразобранных строк\n"
            "/raw_show <raw_id> - детали raw-строки\n"
            "/raw_candidates <raw_id> - безопасные кандидаты case_id\n"
            "/raw_link <raw_id> <case_id> [note] - вручную привязать строку\n"
            "/raw_unlink <raw_id> [note] - снять связь с кейсом и вернуть в pending\n"
            "/raw_ignore <raw_id> [note] - пометить как игнор\n"
            "/raw_pending <raw_id> [note] - вернуть в очередь без снятия связи"
        )

    async def case_help(self, update: Update, context: CallbackContext) -> None:
        if not await self._ensure_admin_access(update, command_name="/case_help"):
            return
        await update.message.reply_text(
            "Поиск по кейсам и raw:\n"
            "/case <query> - кейс по case_id, ШК, таре/передаче или наименованию\n"
            "/case_raw <case_id> - связанные raw-строки кейса\n"
            "/raw_find <query> - raw по ШК, таре/передаче или наименованию\n"
            "Поиск только читает данные. Google Sheets остаётся master для cases."
        )

    async def case_search(self, update: Update, context: CallbackContext) -> None:
        if not await self._ensure_admin_access(update, command_name="/case"):
            return

        query = self._parse_query_text(context.args)
        if not query:
            await update.message.reply_text("Использование: /case <query>")
            return

        search_result = search_cases(
            query,
            limit=DEFAULT_CASE_LIMIT,
            db_path=self.config.db_path,
        )
        cases = search_result["results"]
        if not cases:
            await update.message.reply_text("Кейсы не найдены.")
            return

        if len(cases) == 1 and search_result["match_type"] != "partial":
            await update.message.reply_text(self._format_case_card(cases[0]))
            return

        lines = [
            f"Найдено кейсов: {len(cases)}.",
            self._format_search_hint(search_result),
            *[self._format_case_list_line(case_row) for case_row in cases],
        ]
        if search_result["truncated"]:
            lines.append(f"Показаны первые {DEFAULT_CASE_LIMIT}.")
        await update.message.reply_text("\n".join(lines))

    async def case_raw(self, update: Update, context: CallbackContext) -> None:
        if not await self._ensure_admin_access(update, command_name="/case_raw"):
            return

        case_id = self._parse_query_text(context.args)
        if not case_id:
            await update.message.reply_text("Использование: /case_raw <case_id>")
            return

        case_row = get_search_case_by_case_id(case_id, db_path=self.config.db_path)
        if case_row is None:
            await update.message.reply_text(f"Кейс {case_id} не найден.")
            return

        rows = get_raw_rows_for_case(
            case_id,
            limit=DEFAULT_CASE_RAW_LIMIT + 1,
            db_path=self.config.db_path,
        )
        if not rows:
            await update.message.reply_text(
                f"У кейса {case_id} нет связанных raw-строк."
            )
            return

        lines = [
            f"Связанные raw для {case_id}:",
            *[self._format_case_raw_line(row) for row in rows[:DEFAULT_CASE_RAW_LIMIT]],
        ]
        if len(rows) > DEFAULT_CASE_RAW_LIMIT:
            lines.append(f"Показаны первые {DEFAULT_CASE_RAW_LIMIT}.")
        await update.message.reply_text("\n".join(lines))

    async def raw_find(self, update: Update, context: CallbackContext) -> None:
        if not await self._ensure_admin_access(update, command_name="/raw_find"):
            return

        query = self._parse_query_text(context.args)
        if not query:
            await update.message.reply_text("Использование: /raw_find <query>")
            return

        search_result = search_raw_rows(
            query,
            limit=DEFAULT_RAW_LIMIT,
            db_path=self.config.db_path,
        )
        rows = search_result["results"]
        if not rows:
            await update.message.reply_text("Raw-строки не найдены.")
            return

        lines = [
            f"Найдено raw-строк: {len(rows)}.",
            self._format_search_hint(search_result),
            *[self._format_raw_search_line(row) for row in rows],
        ]
        if search_result["truncated"]:
            lines.append(f"Показаны первые {DEFAULT_RAW_LIMIT}.")
        await update.message.reply_text("\n".join(lines))

    async def raw_queue(self, update: Update, context: CallbackContext) -> None:
        if not await self._ensure_admin_access(update, command_name="/raw_queue"):
            return

        limit, source_kind = self._parse_raw_queue_args(context.args)
        rows = list_unresolved_raw_rows(
            limit=limit,
            source_kind=source_kind,
            db_path=self.config.db_path,
        )
        if not rows:
            await update.message.reply_text("Неразобранных raw-строк сейчас нет.")
            return

        lines = [
            "Очередь raw-строк на разбор:",
            *[self._format_raw_queue_line(row) for row in rows],
        ]
        await update.message.reply_text("\n".join(lines))

    async def raw_show(self, update: Update, context: CallbackContext) -> None:
        if not await self._ensure_admin_access(update, command_name="/raw_show"):
            return

        raw_row_id = self._parse_required_int_arg(context.args, "/raw_show <raw_id>")
        if raw_row_id is None:
            await update.message.reply_text("Использование: /raw_show <raw_id>")
            return

        raw_row = get_raw_row_details(raw_row_id, db_path=self.config.db_path)
        if raw_row is None:
            await update.message.reply_text(f"Raw-строка #{raw_row_id} не найдена.")
            return

        await update.message.reply_text(self._format_raw_details(raw_row))

    async def raw_candidates(self, update: Update, context: CallbackContext) -> None:
        if not await self._ensure_admin_access(update, command_name="/raw_candidates"):
            return

        raw_row_id = self._parse_required_int_arg(
            context.args, "/raw_candidates <raw_id>"
        )
        if raw_row_id is None:
            await update.message.reply_text("Использование: /raw_candidates <raw_id>")
            return

        try:
            candidates = list_raw_row_candidates(
                raw_row_id,
                db_path=self.config.db_path,
            )
        except ValueError:
            await update.message.reply_text(f"Raw-строка #{raw_row_id} не найдена.")
            return

        if not candidates:
            await update.message.reply_text(
                f"Для raw #{raw_row_id} безопасных кандидатов не найдено."
            )
            return

        lines = [f"Кандидаты для raw #{raw_row_id}:"]
        for candidate in candidates:
            lines.append(
                f"{candidate['case_id']} | {candidate['reason']} | "
                f"SHK: {self._display_value(candidate.get('shk'))} | "
                f"тара: {self._display_value(candidate.get('tare_transfer'))} | "
                f"товар: {self._display_value(candidate.get('item_name'))}"
            )
        await update.message.reply_text("\n".join(lines))

    async def raw_link(self, update: Update, context: CallbackContext) -> None:
        if not await self._ensure_admin_access(update, command_name="/raw_link"):
            return

        if len(context.args) < 2:
            await update.message.reply_text(
                "Использование: /raw_link <raw_id> <case_id> [note]"
            )
            return

        try:
            raw_row_id = int(context.args[0])
        except ValueError:
            await update.message.reply_text("raw_id должен быть числом.")
            return

        case_id = context.args[1]
        note = " ".join(context.args[2:]).strip() or None
        try:
            result = manual_link_raw_row(
                raw_row_id=raw_row_id,
                case_id=case_id,
                actor_id=self._actor_id(update),
                note=note,
                db_path=self.config.db_path,
            )
        except ValueError as exc:
            await update.message.reply_text(self._format_review_error(str(exc)))
            return

        if not result["changed"]:
            await update.message.reply_text(
                f"Raw #{raw_row_id} уже привязан к кейсу {case_id} вручную."
            )
            return
        await update.message.reply_text(
            f"Raw #{raw_row_id} привязан к кейсу {case_id}."
        )

    async def raw_ignore(self, update: Update, context: CallbackContext) -> None:
        if not await self._ensure_admin_access(update, command_name="/raw_ignore"):
            return

        raw_row_id = self._parse_required_int_arg(
            context.args, "/raw_ignore <raw_id> [note]"
        )
        if raw_row_id is None:
            await update.message.reply_text(
                "Использование: /raw_ignore <raw_id> [note]"
            )
            return

        note = " ".join(context.args[1:]).strip() or None
        try:
            result = ignore_raw_row(
                raw_row_id=raw_row_id,
                actor_id=self._actor_id(update),
                note=note,
                db_path=self.config.db_path,
            )
        except ValueError as exc:
            await update.message.reply_text(self._format_review_error(str(exc)))
            return

        if not result["changed"]:
            await update.message.reply_text(f"Raw #{raw_row_id} уже помечен как ignored.")
            return
        await update.message.reply_text(f"Raw #{raw_row_id} помечен как ignored.")

    async def raw_unlink(self, update: Update, context: CallbackContext) -> None:
        if not await self._ensure_admin_access(update, command_name="/raw_unlink"):
            return

        raw_row_id = self._parse_required_int_arg(
            context.args, "/raw_unlink <raw_id> [note]"
        )
        if raw_row_id is None:
            await update.message.reply_text(
                "Использование: /raw_unlink <raw_id> [note]"
            )
            return

        note = " ".join(context.args[1:]).strip() or None
        try:
            result = manual_unlink_raw_row(
                raw_row_id=raw_row_id,
                actor_id=self._actor_id(update),
                note=note,
                db_path=self.config.db_path,
            )
        except ValueError as exc:
            await update.message.reply_text(self._format_review_error(str(exc)))
            return

        if not result["changed"]:
            await update.message.reply_text(
                f"У raw-записи {raw_row_id} уже нет связи с кейсом, запись уже в pending."
            )
            return
        if result["had_link"]:
            await update.message.reply_text(
                f"Связь raw-записи {raw_row_id} с кейсом снята. Запись возвращена в pending."
            )
            return
        await update.message.reply_text(
            f"У raw-записи {raw_row_id} уже не было связи с кейсом. Запись возвращена в pending."
        )

    async def raw_pending(self, update: Update, context: CallbackContext) -> None:
        if not await self._ensure_admin_access(update, command_name="/raw_pending"):
            return

        raw_row_id = self._parse_required_int_arg(
            context.args, "/raw_pending <raw_id> [note]"
        )
        if raw_row_id is None:
            await update.message.reply_text(
                "Использование: /raw_pending <raw_id> [note]"
            )
            return

        note = " ".join(context.args[1:]).strip() or None
        try:
            result = mark_raw_row_pending(
                raw_row_id=raw_row_id,
                actor_id=self._actor_id(update),
                note=note,
                db_path=self.config.db_path,
            )
        except ValueError as exc:
            await update.message.reply_text(self._format_review_error(str(exc)))
            return

        if not result["changed"]:
            await update.message.reply_text(f"Raw #{raw_row_id} уже в pending.")
            return
        await update.message.reply_text(
            f"Raw #{raw_row_id} возвращен в pending без снятия связи."
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

    async def _ensure_admin_access(
        self, update: Update, command_name: str | None = None
    ) -> bool:
        user = update.effective_user
        message = update.effective_message
        if not self.config.admin_user_ids:
            logger.warning(
                "Admin command unavailable: ids not configured command=%s user_id=%s username=%s",
                command_name,
                user.id if user else None,
                user.username if user else None,
            )
            if message:
                await message.reply_text(
                    "Команды администратора не настроены. Укажите BOT_ADMIN_IDS или ADMIN_USER_ID."
                )
            return False

        if user and self._is_admin(user.id):
            return True

        logger.warning(
            "Admin access denied command=%s user_id=%s username=%s",
            command_name,
            user.id if user else None,
            user.username if user else None,
        )
        if message:
            await message.reply_text("У вас нет прав для этой команды.")
        return False

    def _parse_raw_queue_args(self, args: list[str]) -> tuple[int, str | None]:
        limit = 10
        source_kind = None
        for arg in args[:2]:
            if arg.isdigit():
                limit = max(1, min(int(arg), 50))
                continue
            normalized = arg.strip().lower()
            if normalized in {SOURCE_KIND_NO_MOVE, SOURCE_KIND_24H}:
                source_kind = normalized
        return limit, source_kind

    def _parse_required_int_arg(
        self, args: list[str], usage: str
    ) -> int | None:
        if not args:
            return None
        try:
            return int(args[0])
        except ValueError:
            logger.warning("Invalid integer argument for %s: %s", usage, args[0])
            return None

    def _parse_query_text(self, args: list[str]) -> str | None:
        query = " ".join(args).strip()
        return query or None

    def _actor_id(self, update: Update) -> str:
        user = update.effective_user
        return str(user.id) if user else "unknown"

    def _display_value(self, value: object) -> str:
        text = str(value).strip() if value is not None else ""
        return text or "—"

    def _short_text(self, value: object, limit: int = 40) -> str:
        text = self._display_value(value)
        if len(text) <= limit:
            return text
        return f"{text[: limit - 1]}…"

    def _format_search_hint(self, search_result: dict) -> str:
        field_labels = {
            "case_id": "case_id",
            "shk": "ШК",
            "tare_transfer": "таре/передаче",
            "item_name": "наименованию",
        }
        type_labels = {
            "exact": "точное",
            "normalized_exact": "нормализованное",
            "partial": "частичное",
        }
        match_field = search_result.get("match_field")
        match_type = search_result.get("match_type")
        if not match_field or not match_type:
            return "Совпадений нет."
        return (
            f"Совпадение: {type_labels.get(match_type, match_type)} по "
            f"{field_labels.get(match_field, match_field)}."
        )

    def _format_case_identity(self, case_row: dict) -> str:
        shk = self._short_text(case_row.get("shk"), 18)
        tare_transfer = self._short_text(case_row.get("tare_transfer"), 18)
        if shk != "—" and tare_transfer != "—":
            return f"{shk} / {tare_transfer}"
        return shk if shk != "—" else tare_transfer

    def _format_case_card(self, case_row: dict) -> str:
        source_parts = []
        if case_row.get("source_sheet_name"):
            source_parts.append(str(case_row["source_sheet_name"]))
        if case_row.get("sheet_row_number"):
            source_parts.append(f"row {case_row['sheet_row_number']}")

        lines = [
            f"Кейс {case_row['case_id']}",
            f"Дата разбора: {self._display_value(case_row.get('review_date'))}",
            f"Аналитик: {self._display_value(case_row.get('analyst'))}",
            f"Наименование: {self._short_text(case_row.get('item_name'), 120)}",
            f"ID виновного: {self._display_value(case_row.get('culprit_id'))}",
            f"Комментарий: {self._short_text(case_row.get('comment_text'), 120)}",
            f"Что предпринято: {self._short_text(case_row.get('action_taken'), 120)}",
            f"Движение товара: {self._short_text(case_row.get('movement_status'), 60)}",
            f"ШК: {self._display_value(case_row.get('shk'))}",
            f"Тара/передача: {self._display_value(case_row.get('tare_transfer'))}",
            f"Склад: {self._display_value(case_row.get('warehouse'))}",
            f"Лист/строка: {self._display_value(' / '.join(source_parts) if source_parts else None)}",
            f"Синхронизация: {self._display_value(case_row.get('last_synced_at'))}",
            f"Raw: /case_raw {case_row['case_id']}",
        ]
        return "\n".join(lines)

    def _format_case_list_line(self, case_row: dict) -> str:
        return (
            f"{case_row['case_id']} | "
            f"{self._display_value(case_row.get('review_date'))} | "
            f"{self._short_text(case_row.get('analyst'), 20)} | "
            f"{self._short_text(case_row.get('item_name'), 32)} | "
            f"{self._format_case_identity(case_row)}"
        )

    def _format_case_raw_line(self, raw_row: dict) -> str:
        return (
            f"#{raw_row['id']} [{self._display_value(raw_row.get('source_kind'))}] "
            f"ШК:{self._short_text(raw_row.get('shk'), 16)} | "
            f"тара:{self._short_text(raw_row.get('tare_transfer'), 16)} | "
            f"{self._short_text(raw_row.get('item_name'), 26)} | "
            f"{self._display_value(raw_row.get('match_method'))}/"
            f"{self._display_value(raw_row.get('match_confidence'))} | "
            f"{self._display_value(raw_row.get('review_status'))}"
        )

    def _format_raw_search_line(self, raw_row: dict) -> str:
        return (
            f"#{raw_row['id']} [{self._display_value(raw_row.get('source_kind'))}] "
            f"ШК:{self._short_text(raw_row.get('shk'), 14)} | "
            f"тара:{self._short_text(raw_row.get('tare_transfer'), 14)} | "
            f"{self._short_text(raw_row.get('item_name'), 24)} | "
            f"case:{self._display_value(raw_row.get('matched_case_id'))} | "
            f"{self._display_value(raw_row.get('review_status'))} | "
            f"conf:{self._display_value(raw_row.get('match_confidence'))}"
        )

    def _format_raw_source(self, raw_row: dict) -> str:
        parts = []
        if raw_row.get("source_file_name"):
            parts.append(str(raw_row["source_file_name"]))
        if raw_row.get("source_sheet_name"):
            parts.append(str(raw_row["source_sheet_name"]))
        if raw_row.get("source_row_number"):
            parts.append(f"row {raw_row['source_row_number']}")
        return " / ".join(parts) or self._short_text(raw_row.get("source_path"), 48)

    def _format_raw_queue_line(self, raw_row: dict) -> str:
        identity = self._short_text(
            raw_row.get("shk")
            or raw_row.get("tare_transfer")
            or raw_row.get("item_name"),
            28,
        )
        return (
            f"#{raw_row['id']} [{self._display_value(raw_row.get('source_kind'))}] "
            f"{identity} | conf={self._display_value(raw_row.get('match_confidence'))} | "
            f"src={self._short_text(self._format_raw_source(raw_row), 44)}"
        )

    def _format_raw_details(self, raw_row: dict) -> str:
        lines = [
            f"Raw #{raw_row['id']}",
            f"Источник: {self._display_value(raw_row.get('source_kind'))}",
            f"Файл/лист: {self._display_value(self._format_raw_source(raw_row))}",
            f"source_path: {self._display_value(raw_row.get('source_path'))}",
            f"SHK: {self._display_value(raw_row.get('shk'))}",
            f"Тара/передача: {self._display_value(raw_row.get('tare_transfer'))}",
            f"Товар: {self._display_value(raw_row.get('item_name'))}",
            f"Сумма: {self._display_value(raw_row.get('amount'))}",
            f"Кол-во SHK: {self._display_value(raw_row.get('qty_shk'))}",
            f"Последнее движение: {self._display_value(raw_row.get('last_movement_at'))}",
            f"Начало списания: {self._display_value(raw_row.get('writeoff_started_at'))}",
            f"matched_case_id: {self._display_value(raw_row.get('matched_case_id'))}",
            f"match_method: {self._display_value(raw_row.get('match_method'))}",
            f"match_confidence: {self._display_value(raw_row.get('match_confidence'))}",
            f"review_status: {self._display_value(raw_row.get('review_status'))}",
            f"review_note: {self._display_value(raw_row.get('review_note'))}",
            f"reviewed_at: {self._display_value(raw_row.get('reviewed_at'))}",
            f"reviewed_by: {self._display_value(raw_row.get('reviewed_by'))}",
            f"manual_linked_at: {self._display_value(raw_row.get('manual_linked_at'))}",
            f"link_reason: {self._display_value(raw_row.get('link_decision_reason'))}",
        ]
        return "\n".join(lines)

    def _format_review_error(self, error_text: str) -> str:
        if error_text.startswith("raw row not found:"):
            raw_row_id = error_text.split(":", 1)[1].strip()
            return f"Raw-строка #{raw_row_id} не найдена."
        if error_text.startswith("case not found:"):
            case_id = error_text.split(":", 1)[1].strip()
            return f"Кейс {case_id} не найден."
        if error_text == "case_id is required":
            return "Нужно указать case_id."
        if error_text == "actor_id is required":
            return "Не удалось определить администратора."
        return "Операцию не удалось выполнить."

    def _is_admin(self, user_id: int) -> bool:
        return str(user_id) in self.config.admin_user_ids
