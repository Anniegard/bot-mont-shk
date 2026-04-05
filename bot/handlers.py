from __future__ import annotations

import logging
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
from bot.services.ai_assistant import AIAssistantError, AIAssistantService
from bot.services.excel import (
    EXPORT_ONLY_TRANSFERS,
    EXPORT_WITH_TRANSFERS,
    EXPORT_WITHOUT_TRANSFERS,
)
from bot.services.ollama_client import OllamaError
from bot.services.processing import (
    ProcessingBusyError,
    ProcessingService,
    SourceFileInfo,
    WorkflowError,
    WorkflowOutcome,
)
from bot.services.yadisk import YaDiskError, yadisk_list_files
from bot.services.yadisk_ingest import SOURCE_KIND_24H, SOURCE_KIND_NO_MOVE

logger = logging.getLogger(__name__)

BUTTON_NO_MOVE = "📦 Без движения"
BUTTON_24H = "⏱ 24 часа (обновить)"
BUTTON_WAREHOUSE_DELAY = "📦 Задержка склада (сводная)"
BUTTON_AI_ASSISTANT = "🤖 AI-ассистент"
BUTTON_ADMIN = "🛠 Админ-панель"
BUTTON_YA_LAST = "☁️ Взять с Я.Диска (последний файл)"
BUTTON_YA_HELP = "📎 Инструкция по загрузке на Диск"

EXPECTED_NO_MOVE = "no_move"
EXPECTED_24H = "24h"
EXPECTED_WAREHOUSE_DELAY_SINGLE = "warehouse_delay_single"
WAREHOUSE_DELAY_MODE_KEY = "warehouse_delay_mode"
NO_MOVE_EXPORT_KEY = "no_move_export_mode"
AI_SESSION_ACTIVE_KEY = "ai_assistant_active"
AI_SESSION_HISTORY_KEY = "ai_assistant_history"

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
WAREHOUSE_DELAY_CALLBACK_PREFIX = "warehouse_delay_mode:"
WAREHOUSE_DELAY_MODE_BUTTONS = {
    f"{WAREHOUSE_DELAY_CALLBACK_PREFIX}single": EXPECTED_WAREHOUSE_DELAY_SINGLE,
    f"{WAREHOUSE_DELAY_CALLBACK_PREFIX}multiple": "warehouse_delay_multiple",
}

MAX_TG_UPLOAD_BYTES = 20 * 1024 * 1024
class BotHandlers:
    def __init__(self, config: Config, gspread_client):
        self.config = config
        self.gc = gspread_client
        self.last_no_move_product_ids = set()

        root_dir = Path(__file__).resolve().parent.parent
        self.workdir = root_dir
        self.data_dir = root_dir / "data"
        self.block_ids_path = self.data_dir / "block_ids.txt"
        self.snapshot_path = self.data_dir / "last_24h_snapshot.json"
        self.snapshot_meta_path = self.data_dir / "last_24h_meta.json"
        self.no_move_map_path = self.data_dir
        self.processing_service = ProcessingService(
            config=config,
            gspread_client=gspread_client,
            workdir=root_dir,
            block_ids_path=self.block_ids_path,
            snapshot_path=self.snapshot_path,
            snapshot_meta_path=self.snapshot_meta_path,
            no_move_map_path=self.no_move_map_path,
        )
        self.processing_lock = self.processing_service.lock
        self.ai_assistant_service = AIAssistantService(
            config=config,
            processing_service=self.processing_service,
        )

        self.reply_keyboard = ReplyKeyboardMarkup(
            [
                [BUTTON_NO_MOVE],
                [BUTTON_24H],
                [BUTTON_WAREHOUSE_DELAY],
                [BUTTON_YA_LAST],
                [BUTTON_YA_HELP],
                [BUTTON_ADMIN],
            ],
            resize_keyboard=True,
        )
        self.admin_reply_keyboard = ReplyKeyboardMarkup(
            [
                [BUTTON_NO_MOVE],
                [BUTTON_24H],
                [BUTTON_WAREHOUSE_DELAY],
                [BUTTON_YA_LAST],
                [BUTTON_YA_HELP],
                [BUTTON_AI_ASSISTANT],
                [BUTTON_ADMIN],
            ],
            resize_keyboard=True,
        )

        logger.debug(
            "Yandex OAuth: %s; dirs: no_move=%s h24=%s warehouse_delay=%s",
            "configured" if self.config.yandex_oauth_token else "none",
            self.config.yandex_no_move_dir,
            self.config.yandex_24h_dir,
            self.config.yandex_warehouse_delay_dir,
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
                filters.Regex(f"^{re.escape(BUTTON_WAREHOUSE_DELAY)}$"),
                self.handle_warehouse_delay_summary,
            )
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
            MessageHandler(
                filters.Regex(f"^{re.escape(BUTTON_AI_ASSISTANT)}$"),
                self.enter_ai_assistant,
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
        application.add_handler(
            CallbackQueryHandler(
                self.warehouse_delay_mode_selected,
                pattern=f"^{WAREHOUSE_DELAY_CALLBACK_PREFIX}",
            )
        )
        application.add_handler(CallbackQueryHandler(self.admin_button_handler))

    async def start(self, update: Update, context: CallbackContext) -> None:
        user = update.effective_user
        context.user_data["expected_upload"] = None
        context.user_data[WAREHOUSE_DELAY_MODE_KEY] = None
        self._reset_ai_session(context)
        message = (
            "Привет! Я могу обработать Excel:\n"
            "• 📦 Без движения — основной файл с гофрой/идентификаторами.\n"
            "• ⏱ 24 часа — файл прогноза списаний, обновляет правую таблицу.\n"
            "• 📦 Задержка склада (сводная) — умеет обработать один сводный файл или все файлы из папки Я.Диска.\n"
            "Выберите режим кнопкой снизу и пришлите файл (.xlsx до 20 МБ) или заберите последний файл с Я.Диска кнопкой ниже.\n"
            "Между строками выгрузки будет пустая строка для удобного CTRL+A."
        )
        if user and self._is_admin(user.id):
            message += (
                "\n• 🤖 AI-ассистент — вопросы по данным с Я.Диска "
                "(без движения, 24ч, задержка склада; несколько файлов при необходимости)."
            )
        logger.info("Команда /start user_id=%s username=%s", user.id, user.username)
        await update.message.reply_text(
            message,
            reply_markup=self._reply_keyboard_for_user(user.id if user else None),
        )

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
            "Выберите действие.",
            reply_markup=InlineKeyboardMarkup(admin_keyboard),
        )

    async def enter_ai_assistant(
        self, update: Update, context: CallbackContext
    ) -> None:
        if not await self._ensure_admin_access(update, command_name=BUTTON_AI_ASSISTANT):
            return

        self._reset_ai_session(context)
        context.user_data["expected_upload"] = None
        context.user_data[NO_MOVE_EXPORT_KEY] = None
        context.user_data[WAREHOUSE_DELAY_MODE_KEY] = None

        try:
            self.ai_assistant_service.ensure_configured()
        except (AIAssistantError, OllamaError) as exc:
            await update.message.reply_text(
                str(exc),
                reply_markup=self._reply_keyboard_for_user(update.effective_user.id),
            )
            return

        context.user_data[AI_SESSION_ACTIVE_KEY] = True
        await update.message.reply_text(
            "Режим AI-ассистента включён.\n"
            "Пишите вопросы по данным с Я.Диска: «Без движения», «24 часа», «Задержка склада». "
            "Ассистент сам выберет нужные файлы (в т.ч. несколько) или ответит без выгрузки, "
            "если в данных нет необходимости.\n"
            "Для выхода нажмите другую рабочую кнопку или выполните /start.",
            reply_markup=self._reply_keyboard_for_user(update.effective_user.id),
        )

    async def _reply_runtime_db_features_disabled(
        self, update: Update, command_name: str
    ) -> None:
        logger.info("Runtime DB features disabled; ignoring command=%s", command_name)
        message = update.effective_message
        if message:
            await message.reply_text(
                "Команда недоступна: runtime DB features disabled."
            )

    async def raw_help(self, update: Update, context: CallbackContext) -> None:
        if not await self._ensure_admin_access(update, command_name="/raw_help"):
            return
        await self._reply_runtime_db_features_disabled(update, "/raw_help")

    async def case_help(self, update: Update, context: CallbackContext) -> None:
        if not await self._ensure_admin_access(update, command_name="/case_help"):
            return
        await self._reply_runtime_db_features_disabled(update, "/case_help")

    async def case_search(self, update: Update, context: CallbackContext) -> None:
        if not await self._ensure_admin_access(update, command_name="/case"):
            return
        await self._reply_runtime_db_features_disabled(update, "/case")

    async def case_raw(self, update: Update, context: CallbackContext) -> None:
        if not await self._ensure_admin_access(update, command_name="/case_raw"):
            return
        await self._reply_runtime_db_features_disabled(update, "/case_raw")

    async def raw_find(self, update: Update, context: CallbackContext) -> None:
        if not await self._ensure_admin_access(update, command_name="/raw_find"):
            return
        await self._reply_runtime_db_features_disabled(update, "/raw_find")

    async def raw_queue(self, update: Update, context: CallbackContext) -> None:
        if not await self._ensure_admin_access(update, command_name="/raw_queue"):
            return
        await self._reply_runtime_db_features_disabled(update, "/raw_queue")

    async def raw_show(self, update: Update, context: CallbackContext) -> None:
        if not await self._ensure_admin_access(update, command_name="/raw_show"):
            return
        await self._reply_runtime_db_features_disabled(update, "/raw_show")

    async def raw_candidates(self, update: Update, context: CallbackContext) -> None:
        if not await self._ensure_admin_access(update, command_name="/raw_candidates"):
            return
        await self._reply_runtime_db_features_disabled(update, "/raw_candidates")

    async def raw_link(self, update: Update, context: CallbackContext) -> None:
        if not await self._ensure_admin_access(update, command_name="/raw_link"):
            return
        await self._reply_runtime_db_features_disabled(update, "/raw_link")

    async def raw_ignore(self, update: Update, context: CallbackContext) -> None:
        if not await self._ensure_admin_access(update, command_name="/raw_ignore"):
            return
        await self._reply_runtime_db_features_disabled(update, "/raw_ignore")

    async def raw_unlink(self, update: Update, context: CallbackContext) -> None:
        if not await self._ensure_admin_access(update, command_name="/raw_unlink"):
            return
        await self._reply_runtime_db_features_disabled(update, "/raw_unlink")

    async def raw_pending(self, update: Update, context: CallbackContext) -> None:
        if not await self._ensure_admin_access(update, command_name="/raw_pending"):
            return
        await self._reply_runtime_db_features_disabled(update, "/raw_pending")

    async def select_no_move(self, update: Update, context: CallbackContext) -> None:
        self._reset_ai_session(context)
        context.user_data["expected_upload"] = EXPECTED_NO_MOVE
        context.user_data[NO_MOVE_EXPORT_KEY] = None
        context.user_data[WAREHOUSE_DELAY_MODE_KEY] = None
        user = update.effective_user
        logger.info(
            "Выбран режим без движения user_id=%s username=%s", user.id, user.username
        )
        await update.message.reply_text(
            "Выберите тип выгрузки для режима «Без движения»:",
            reply_markup=self._no_move_mode_keyboard(),
        )

    async def select_24h(self, update: Update, context: CallbackContext) -> None:
        self._reset_ai_session(context)
        context.user_data["expected_upload"] = EXPECTED_24H
        context.user_data[WAREHOUSE_DELAY_MODE_KEY] = None
        user = update.effective_user
        logger.info("Выбран режим 24ч user_id=%s username=%s", user.id, user.username)
        await update.message.reply_text(
            "Ок, пришли Excel «24 часа» (документ до 20 МБ) или нажми «☁️ Взять с Я.Диска».",
            reply_markup=self._reply_keyboard_for_user(user.id if user else None),
        )

    async def handle_warehouse_delay_summary(
        self, update: Update, context: CallbackContext
    ) -> None:
        self._reset_ai_session(context)
        context.user_data["expected_upload"] = None
        context.user_data[WAREHOUSE_DELAY_MODE_KEY] = None
        await update.message.reply_text(
            "Выберите способ обработки:",
            reply_markup=self._warehouse_delay_mode_keyboard(),
        )

    def _warehouse_delay_mode_keyboard(self) -> InlineKeyboardMarkup:
        return InlineKeyboardMarkup(
            [
                [
                    InlineKeyboardButton(
                        "Из одного файла",
                        callback_data=f"{WAREHOUSE_DELAY_CALLBACK_PREFIX}single",
                    ),
                    InlineKeyboardButton(
                        "Из нескольких файлов",
                        callback_data=f"{WAREHOUSE_DELAY_CALLBACK_PREFIX}multiple",
                    ),
                ]
            ]
        )

    async def warehouse_delay_mode_selected(
        self, update: Update, context: CallbackContext
    ) -> None:
        query = update.callback_query
        await query.answer()
        selected_mode = WAREHOUSE_DELAY_MODE_BUTTONS.get(query.data)
        if not selected_mode:
            return

        context.user_data[WAREHOUSE_DELAY_MODE_KEY] = selected_mode
        user = query.from_user
        logger.info(
            "Выбран подрежим warehouse delay user_id=%s username=%s mode=%s",
            user.id,
            user.username,
            selected_mode,
        )

        try:
            await query.edit_message_reply_markup(reply_markup=None)
        except Exception:
            pass

        if selected_mode == EXPECTED_WAREHOUSE_DELAY_SINGLE:
            context.user_data["expected_upload"] = EXPECTED_WAREHOUSE_DELAY_SINGLE
            await query.message.reply_text(
                "Пришлите один Excel-файл или нажмите «☁️ Взять с Я.Диска (последний файл)».\n"
                "Для этого режима нужен единый сводный файл с колонкой «Блок».",
                reply_markup=self._reply_keyboard_for_user(user.id if user else None),
            )
            return

        context.user_data["expected_upload"] = None
        await self._run_warehouse_delay_multiple(query.message, user)

    async def _run_warehouse_delay_multiple(self, message, user) -> None:
        if self.processing_lock.locked():
            await message.reply_text(
                "Сейчас выполняется другая обработка. Повторите чуть позже."
            )
            return

        folder = (
            self.config.yandex_warehouse_delay_dir
            or "disk:/BOT_UPLOADS/warehouse_delay/"
        )
        status_message = await message.reply_text(
            f"Начал обработку файлов из {folder}..."
        )

        async with self.processing_service.processing_slot():
            try:
                files = await yadisk_list_files(
                    self.config.yandex_oauth_token or "",
                    folder,
                    self.config.yandex_allowed_exts,
                )
                aggregation = await self._download_and_process_warehouse_delay_files(files)
                outcome = await self.processing_service.build_warehouse_delay_multiple_outcome(
                    aggregation
                )
                await self._edit_status_with_outcome(status_message, outcome)
                logger.info(
                    "Warehouse delay summary updated user_id=%s username=%s worksheet=%s",
                    user.id,
                    user.username,
                    self.config.warehouse_delay_worksheet_name,
                )
            except (ProcessingBusyError, WorkflowError, YaDiskError) as exc:
                logger.warning(
                    "Warehouse delay processing warning user_id=%s username=%s: %s",
                    user.id,
                    user.username,
                    exc,
                )
                await status_message.edit_text(str(exc))
            except Exception:
                logger.exception(
                    "Warehouse delay processing failed user_id=%s username=%s",
                    user.id,
                    user.username,
                )
                await status_message.edit_text(
                    "Произошла ошибка при обработке сводной задержки склада. Подробности в логах."
                )

    async def _download_and_process_warehouse_delay_files(self, files):
        return await self.processing_service.download_and_aggregate_warehouse_delay_files(
            files
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
            f"Режим выбран: {label.lower()}. Пришлите Excel-файл или используйте Я.Диск.",
            reply_markup=self._reply_keyboard_for_user(user.id if user else None),
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

    async def _handle_warehouse_delay_single_file(
        self,
        update: Update,
        context: CallbackContext,
        file_path: str,
        file_info: dict,
    ) -> None:
        context.user_data["expected_upload"] = None
        status_message = await update.message.reply_text(
            "Читаю сводный файл задержки склада..."
        )
        info = SourceFileInfo(
            filename=file_info.get("filename") or Path(file_path).name,
            size=file_info.get("size"),
            source=file_info.get("source") or "local_file",
            source_path=file_info.get("source_path") or file_path,
        )
        try:
            async with self.processing_service.processing_slot():
                outcome = await self.processing_service.process_local_source(
                    EXPECTED_WAREHOUSE_DELAY_SINGLE,
                    file_path,
                    info,
                )
            await self._edit_status_with_outcome(status_message, outcome)
        except (ProcessingBusyError, WorkflowError, YaDiskError) as exc:
            await status_message.edit_text(str(exc))
        except Exception:
            logger.exception(
                "Warehouse delay single processing failed file=%s",
                file_info.get("filename"),
            )
            await status_message.edit_text(
                "Произошла ошибка при обработке сводной задержки склада. Подробности в логах."
            )

    async def handle_yadisk_help(
        self, update: Update, context: CallbackContext
    ) -> None:
        text = self.processing_service.build_yadisk_help_text()
        user = update.effective_user
        await update.message.reply_text(
            text,
            reply_markup=self._reply_keyboard_for_user(user.id if user else None),
        )

    async def handle_yadisk_latest(
        self, update: Update, context: CallbackContext
    ) -> None:
        user = update.effective_user
        if context.user_data.get(AI_SESSION_ACTIVE_KEY):
            await update.message.reply_text(
                "В AI-режиме отдельно нажимать Я.Диск не нужно. "
                "Просто задайте вопрос, и бот сам возьмёт последний файл "
                "«Без движения».",
                reply_markup=self._reply_keyboard_for_user(user.id if user else None),
            )
            return
        expected = context.user_data.get("expected_upload")
        if not expected:
            await update.message.reply_text(
                "Сначала выберите режим: 📦 Без движения, ⏱ 24 часа или «📦 Задержка склада (сводная) → Из одного файла».",
                reply_markup=self._reply_keyboard_for_user(user.id if user else None),
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

        status_message = await update.message.reply_text("Ищу последний файл на Я.Диске...")

        try:
            async with self.processing_service.processing_slot():
                outcome = await self.processing_service.process_latest_yadisk_file(
                    expected,
                    no_move_export_mode=context.user_data.get(NO_MOVE_EXPORT_KEY),
                )
            context.user_data["expected_upload"] = None
            await self._edit_status_with_outcome(status_message, outcome)
            logger.info(
                "YaDisk файл обработан: user_id=%s username=%s mode=%s name=%s",
                user.id,
                user.username,
                expected,
                outcome.source_name,
            )
        except (ProcessingBusyError, WorkflowError, YaDiskError) as exc:
            logger.warning("Ошибка Яндекс.Диск user_id=%s: %s", user.id, exc)
            await status_message.edit_text(str(exc))
        except Exception:
            logger.exception("Сбой при загрузке с Я.Диска user_id=%s", user.id)
            await status_message.edit_text("Не удалось получить файл с Я.Диска.")

    async def handle_text(self, update: Update, context: CallbackContext) -> None:
        if context.user_data.get(AI_SESSION_ACTIVE_KEY):
            await self.handle_ai_question(update, context)
            return
        user = update.effective_user
        await update.message.reply_text(
            "Выберите режим кнопкой снизу и пришлите Excel как документ (до 20 МБ) "
            "или воспользуйтесь «☁️ Взять с Я.Диска». Загрузка по ссылке отключена.",
            reply_markup=self._reply_keyboard_for_user(user.id if user else None),
        )

    async def handle_ai_question(
        self, update: Update, context: CallbackContext
    ) -> None:
        user = update.effective_user
        if not user or not self._is_admin(user.id):
            self._reset_ai_session(context)
            await update.message.reply_text("У вас нет прав для AI-ассистента.")
            return

        question = (update.message.text or "").strip()
        if not question:
            await update.message.reply_text(
                "Напишите вопрос по выгрузкам с Я.Диска (без движения, 24ч, задержка склада).",
                reply_markup=self._reply_keyboard_for_user(user.id),
            )
            return

        history = context.user_data.get(AI_SESSION_HISTORY_KEY) or []
        status_message = await update.message.reply_text("Планирую запрос и смотрю каталог Я.Диска…")
        try:
            response = await self.ai_assistant_service.answer_question(
                question=question,
                history=history,
                status=status_message,
            )
            context.user_data[AI_SESSION_HISTORY_KEY] = (
                self.ai_assistant_service.trim_history(
                    [
                        *history,
                        {"role": "user", "content": question},
                        {"role": "assistant", "content": response.text},
                    ]
                )
            )
            logger.info(
                "AI assistant answered user_id=%s username=%s source=%s matched_rows=%s",
                user.id,
                user.username,
                response.source_name,
                response.matched_rows,
            )
            await status_message.edit_text(response.text)
        except (
            ProcessingBusyError,
            AIAssistantError,
            OllamaError,
            YaDiskError,
        ) as exc:
            logger.warning(
                "AI assistant warning user_id=%s username=%s: %s",
                user.id,
                user.username,
                exc,
            )
            await status_message.edit_text(str(exc))
        except Exception:
            logger.exception(
                "AI assistant failed user_id=%s username=%s",
                user.id,
                user.username,
            )
            await status_message.edit_text(
                "Не удалось получить ответ AI-ассистента. Подробности в логах."
            )

    async def handle_file(self, update: Update, context: CallbackContext) -> None:
        user = update.effective_user
        document = update.message.document
        if context.user_data.get(AI_SESSION_ACTIVE_KEY):
            await update.message.reply_text(
                "В режиме AI-ассистента файл загружать не нужно. "
                "Напишите вопрос — данные подтянутся с Я.Диска при необходимости.",
                reply_markup=self._reply_keyboard_for_user(user.id if user else None),
            )
            return
        expected = context.user_data.get("expected_upload")

        if not expected:
            await update.message.reply_text(
                "Сначала выберите режим: 📦 Без движения, ⏱ 24 часа или «📦 Задержка склада (сводная) → Из одного файла».",
                reply_markup=self._reply_keyboard_for_user(user.id if user else None),
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

        status_message = None
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
            target_path = self.processing_service.make_temp_path(
                f"upload_{user.id}",
                Path(original_name).suffix or ".xlsx",
            )
            file_path = await tg_file.download_to_drive(custom_path=str(target_path))
            status_message = await update.message.reply_text(
                self._status_text_for_expected(expected)
            )
            try:
                file_info = SourceFileInfo(
                    filename=original_name,
                    size=document.file_size,
                    source="telegram_document",
                    source_path=f"telegram:{document.file_unique_id or document.file_id}",
                )
                async with self.processing_service.processing_slot():
                    outcome = await self.processing_service.process_local_source(
                        expected,
                        file_path,
                        file_info,
                        no_move_export_mode=context.user_data.get(NO_MOVE_EXPORT_KEY),
                    )
                context.user_data["expected_upload"] = None
                await self._edit_status_with_outcome(status_message, outcome)
            finally:
                Path(file_path).unlink(missing_ok=True)
        except (ProcessingBusyError, WorkflowError, YaDiskError) as exc:
            logger.warning(
                "Ошибка загрузки файла user_id=%s username=%s: %s",
                user.id,
                user.username,
                exc,
            )
            if status_message is not None:
                await status_message.edit_text(str(exc))
            else:
                await update.message.reply_text(str(exc))
        except Exception:
            logger.exception(
                "Ошибка загрузки файла user_id=%s username=%s", user.id, user.username
            )
            await update.message.reply_text(
                "Ошибка при загрузке файла. Попробуйте еще раз."
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
                reply_markup=self._reply_keyboard_for_user(user.id),
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
            "Файл слишком большой для скачивания через Telegram (лимит 20 МБ) или недоступен.\n"
            "Загрузите файл в нужную папку на Яндекс.Диске "
            "(см. «📎 Инструкция по загрузке на Диск»), затем после выбора режима "
            "нажмите «☁️ Взять с Я.Диска (последний файл)»."
        )
        user = update.effective_user
        await update.message.reply_text(
            text,
            reply_markup=self._reply_keyboard_for_user(user.id if user else None),
        )

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

    def _parse_required_int_arg(self, args: list[str], usage: str) -> int | None:
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

    async def _edit_status_with_outcome(self, status_message, outcome: WorkflowOutcome) -> None:
        kwargs: dict[str, object] = {}
        if outcome.parse_mode:
            kwargs["parse_mode"] = outcome.parse_mode
        if outcome.disable_web_page_preview:
            kwargs["disable_web_page_preview"] = True
        message = outcome.message
        if outcome.sheet_url:
            message += f'\n<a href="{escape(outcome.sheet_url)}">Ссылка на таблицу</a>'
        await status_message.edit_text(message, **kwargs)

    def _sheet_link_html(self) -> str | None:
        return self.processing_service.sheet_link_html()

    def _status_text_for_expected(self, expected: str) -> str:
        if expected == EXPECTED_NO_MOVE:
            return "Загружаю файл «без движения»..."
        if expected == EXPECTED_24H:
            return "Загружаю файл «24 часа»..."
        if expected == EXPECTED_WAREHOUSE_DELAY_SINGLE:
            return "Загружаю сводный файл задержки склада..."
        return "Загружаю файл..."

    def _is_admin(self, user_id: int) -> bool:
        return str(user_id) in self.config.admin_user_ids

    def _reply_keyboard_for_user(self, user_id: int | None) -> ReplyKeyboardMarkup:
        if user_id is not None and self._is_admin(user_id):
            return self.admin_reply_keyboard
        return self.reply_keyboard

    def _reset_ai_session(self, context: CallbackContext) -> None:
        context.user_data[AI_SESSION_ACTIVE_KEY] = False
        context.user_data[AI_SESSION_HISTORY_KEY] = []
