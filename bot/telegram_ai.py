from __future__ import annotations

import logging
from pathlib import Path
from uuid import uuid4

from telegram import Update
from telegram.error import BadRequest
from telegram.ext import CallbackContext

from bot.config import Config
from bot.services.ai.assistant import AIAssistantError, AIAssistantService
from bot.services.ai.types import AISessionState, AISourceRef
from bot.services.processing import ProcessingService

logger = logging.getLogger(__name__)

AI_SESSION_KEY = "ai_session_state"


class TelegramAIController:
    def __init__(
        self,
        config: Config,
        processing_service: ProcessingService,
        is_admin,
    ) -> None:
        self.config = config
        self.processing_service = processing_service
        self.ai_service = AIAssistantService(config, processing_service)
        self._is_admin = is_admin

    def get_session(self, context: CallbackContext) -> AISessionState:
        session = context.user_data.get(AI_SESSION_KEY)
        if isinstance(session, AISessionState):
            return session
        session = AISessionState()
        context.user_data[AI_SESSION_KEY] = session
        return session

    def reset_session(self, context: CallbackContext) -> None:
        session = self.get_session(context)
        for path_str in list(session.uploaded_paths):
            self.processing_service._cleanup_temp_artifacts(Path(path_str))
        session.active = False
        session.source_refs.clear()
        session.history.clear()
        session.uploaded_paths.clear()

    def has_active_session(self, context: CallbackContext) -> bool:
        return self.get_session(context).active

    async def enter_ai_mode(self, update: Update, context: CallbackContext) -> None:
        self._ensure_admin(update)
        self.reset_session(context)
        self.ai_service.ensure_configured()
        session = self.get_session(context)
        session.active = True
        await update.effective_message.reply_text(
            "AI-режим включён.\n"
            "Команды:\n"
            "/ai_use no_move\n"
            "/ai_use 24h\n"
            "/ai_use warehouse_delay\n"
            "/ai_reset\n"
            "/ai_exit\n\n"
            "Также можно прислать .xlsx/.xls/.csv/.zip как AI-источник и затем задать вопрос."
        )

    async def exit_ai_mode(self, update: Update, context: CallbackContext) -> None:
        self.reset_session(context)
        await update.effective_message.reply_text("AI-режим выключен.")

    async def reset_ai_context(self, update: Update, context: CallbackContext) -> None:
        self._ensure_admin(update)
        session = self.get_session(context)
        active = session.active
        self.reset_session(context)
        session = self.get_session(context)
        session.active = active
        await update.effective_message.reply_text("AI-контекст очищен.")

    async def add_named_source(
        self,
        update: Update,
        context: CallbackContext,
        source_name: str | None,
    ) -> None:
        self._ensure_admin(update)
        session = self.get_session(context)
        if not session.active:
            raise AIAssistantError("Сначала включите AI-режим командой /ai.")
        normalized = (source_name or "").strip().lower()
        mapping = {
            "no_move": AISourceRef(
                kind="yadisk_no_move_latest",
                label="Последний no_move с Я.Диска",
            ),
            "24h": AISourceRef(
                kind="project_24h",
                label="Проектный источник 24ч",
            ),
            "warehouse_delay": AISourceRef(
                kind="yadisk_warehouse_latest",
                label="Последняя сводная warehouse_delay с Я.Диска",
            ),
        }
        source_ref = mapping.get(normalized)
        if source_ref is None:
            raise AIAssistantError(
                "Поддерживаются только: /ai_use no_move, /ai_use 24h, /ai_use warehouse_delay."
            )
        self._append_unique_source(session, source_ref)
        await update.effective_message.reply_text(f"Источник добавлен: {source_ref.label}.")

    async def handle_uploaded_source(self, update: Update, context: CallbackContext) -> bool:
        session = self.get_session(context)
        if not session.active:
            return False

        document = update.effective_message.document
        if document is None:
            return False
        if document.file_size and document.file_size > self.config.ai_max_file_mb * 1024 * 1024:
            raise AIAssistantError(
                f"Файл слишком большой для AI-режима. Лимит: {self.config.ai_max_file_mb} МБ."
            )

        try:
            tg_file = await document.get_file()
        except BadRequest as exc:
            raise AIAssistantError("Не удалось скачать файл из Telegram для AI-режима.") from exc

        original_name = document.file_name or "ai_source.xlsx"
        target_path = self.processing_service.make_temp_path(
            f"ai_upload_{update.effective_user.id}",
            Path(original_name).suffix or ".xlsx",
        )
        downloaded_path = Path(await tg_file.download_to_drive(custom_path=str(target_path)))
        session.uploaded_paths.append(str(downloaded_path))
        self._append_unique_source(
            session,
            AISourceRef(
                kind="uploaded_file",
                label=f"Загруженный файл: {original_name}",
                filename=original_name,
                file_path=downloaded_path,
            ),
        )
        await update.effective_message.reply_text(
            f"AI-источник сохранён: {original_name}. Теперь задайте вопрос или добавьте ещё источник."
        )
        return True

    async def handle_question(self, update: Update, context: CallbackContext) -> bool:
        session = self.get_session(context)
        if not session.active:
            return False

        question = (update.effective_message.text or "").strip()
        if not question:
            raise AIAssistantError("Введите вопрос для AI-ассистента.")

        status = await update.effective_message.reply_text("Собираю контекст и готовлю ответ...")
        request_id = uuid4().hex[:12]
        result = await self.ai_service.answer_question(
            request_id=request_id,
            user_id=str(update.effective_user.id),
            username=update.effective_user.username,
            question=question,
            source_refs=list(session.source_refs),
            history=list(session.history),
        )
        session.history = self.ai_service.trim_history(
            session.history
            + [{"role": "user", "content": question}]
            + [{"role": "assistant", "content": result.answer_text}]
        )
        await status.edit_text(result.answer_text)
        return True

    def _append_unique_source(self, session: AISessionState, source_ref: AISourceRef) -> None:
        for existing in session.source_refs:
            if existing.kind == source_ref.kind and existing.label == source_ref.label:
                return
        if len(session.source_refs) >= self.config.ai_max_files_per_request:
            raise AIAssistantError(
                f"Превышен лимит источников для одного AI-запроса: {self.config.ai_max_files_per_request}."
            )
        session.source_refs.append(source_ref)

    def _ensure_admin(self, update: Update) -> None:
        user = update.effective_user
        if not user or not self._is_admin(user.id):
            raise AIAssistantError("У вас нет прав для этой команды.")
