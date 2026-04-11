from __future__ import annotations

import asyncio
import logging
import time

from bot.config import Config
from bot.services.ai.context_builder import AIContextBuilder
from bot.services.ai.openai_provider import OpenAIProvider
from bot.services.ai.prompts import build_messages
from bot.services.ai.provider import (
    AIProviderConfigurationError,
    AIProviderError,
    AIProviderTemporaryError,
    LLMClient,
)
from bot.services.ai.sources import AISourceError, AISourceLoader
from bot.services.ai.types import AIAssistantResult, AILimits, AISourceRef
from bot.services.processing import ProcessingService

logger = logging.getLogger(__name__)


class AIAssistantError(Exception):
    pass


class AIAssistantService:
    def __init__(
        self,
        config: Config,
        processing_service: ProcessingService,
        provider: LLMClient | None = None,
    ) -> None:
        self.config = config
        self.processing_service = processing_service
        self.provider = provider or self._build_provider()
        self.source_loader = AISourceLoader(config, processing_service)
        self.context_builder = AIContextBuilder(config)
        self.limits = AILimits(
            max_files_per_request=config.ai_max_files_per_request,
            max_file_bytes=config.ai_max_file_mb * 1024 * 1024,
            max_rows_per_source=config.ai_max_rows_per_source,
            max_scan_rows_per_source=config.ai_max_scan_rows_per_source,
            max_context_chars=config.ai_max_context_chars,
            max_history_messages=config.ai_max_history_messages,
            max_answer_chars=config.ai_max_answer_chars,
            max_retries=config.ai_max_retries,
            retry_backoff_ms=config.ai_retry_backoff_ms,
        )
        self._semaphore = asyncio.Semaphore(config.ai_max_concurrent_requests)

    def ensure_configured(self) -> None:
        if not self.config.ai_enabled:
            raise AIAssistantError("AI-режим выключен через AI_ENABLED.")
        if self.config.ai_provider != "openai":
            raise AIAssistantError(
                f"Неподдерживаемый AI_PROVIDER: {self.config.ai_provider}."
            )
        if not self.config.openai_api_key:
            raise AIAssistantError("OPENAI_API_KEY не настроен.")
        if not self.config.openai_model:
            raise AIAssistantError("OPENAI_MODEL не настроен.")

    async def answer_question(
        self,
        *,
        request_id: str,
        user_id: str,
        username: str | None,
        question: str,
        source_refs: list[AISourceRef],
        history: list[dict[str, str]],
    ) -> AIAssistantResult:
        normalized_question = question.strip()
        if not normalized_question:
            raise AIAssistantError("Введите вопрос для AI-ассистента.")
        self.ensure_configured()
        if not source_refs:
            raise AIAssistantError(
                "Сначала добавьте источник через /ai_use или загрузите файл в AI-режиме."
            )

        logger.info(
            "event=ai_request_started request_id=%s user_id=%s username=%s source_count=%s source_types=%s question_chars=%s",
            request_id,
            user_id,
            username or "",
            len(source_refs),
            ",".join(source.kind for source in source_refs),
            len(normalized_question),
        )

        started = time.perf_counter()
        async with self._semaphore:
            try:
                loaded = await self.source_loader.load_sources(source_refs)
                context_package = self.context_builder.build_context(
                    list(loaded.extractions),
                    normalized_question,
                )
                logger.info(
                    "event=ai_context_built request_id=%s rows_scanned=%s rows_selected=%s context_chars=%s truncated=%s",
                    request_id,
                    context_package.total_rows_scanned,
                    context_package.total_rows_selected,
                    len(context_package.text),
                    context_package.truncation_count > 0,
                )
                provider_result = await self.provider.complete(
                    messages=build_messages(
                        question=normalized_question,
                        history=self.trim_history(history),
                        context_package=context_package,
                    ),
                    max_output_tokens=max(128, self.config.ai_max_answer_chars // 4),
                )
            except (AISourceError, AIProviderConfigurationError) as exc:
                logger.warning(
                    "event=ai_request_failed request_id=%s stage=prepare error_type=%s",
                    request_id,
                    type(exc).__name__,
                )
                raise AIAssistantError(str(exc)) from exc
            except (AIProviderTemporaryError, AIProviderError) as exc:
                logger.warning(
                    "event=ai_request_failed request_id=%s stage=provider error_type=%s",
                    request_id,
                    type(exc).__name__,
                )
                raise AIAssistantError(str(exc)) from exc
            finally:
                if "loaded" in locals():
                    self.source_loader.cleanup(loaded.cleanup_paths)

        latency_ms = int((time.perf_counter() - started) * 1000)
        notes = list(
            self._build_notes(
                source_count=len(source_refs),
                truncation_count=context_package.truncation_count,
                rows_selected=context_package.total_rows_selected,
            )
        )
        answer_text = self._finalize_answer(
            source_refs=source_refs,
            provider_text=provider_result.text,
            notes=tuple(notes),
        )
        logger.info(
            "event=ai_provider_result request_id=%s provider=%s model=%s latency_ms=%s answer_chars=%s retry_count=%s status=success total_latency_ms=%s",
            request_id,
            self.config.ai_provider,
            provider_result.model,
            provider_result.latency_ms,
            len(answer_text),
            max(0, provider_result.attempt_count - 1),
            latency_ms,
        )
        return AIAssistantResult(
            answer_text=answer_text,
            model=provider_result.model,
            source_labels=tuple(source.label for source in source_refs),
            total_rows_scanned=context_package.total_rows_scanned,
            total_rows_selected=context_package.total_rows_selected,
            context_chars=len(context_package.text),
            truncation_count=context_package.truncation_count,
            provider_attempt_count=provider_result.attempt_count,
            provider_latency_ms=provider_result.latency_ms,
            notes=tuple(notes),
        )

    def trim_history(self, history: list[dict[str, str]]) -> list[dict[str, str]]:
        limit = self.config.ai_max_history_messages
        cleaned = [
            {"role": str(item.get("role")), "content": str(item.get("content"))}
            for item in history
            if item.get("role") in {"user", "assistant"} and (item.get("content") or "").strip()
        ]
        if limit <= 0:
            return []
        return cleaned[-limit:]

    def _build_provider(self) -> LLMClient:
        if self.config.ai_provider == "openai":
            return OpenAIProvider(self.config)
        raise AIAssistantError(f"Неподдерживаемый AI_PROVIDER: {self.config.ai_provider}.")

    def _finalize_answer(
        self,
        *,
        source_refs: list[AISourceRef],
        provider_text: str,
        notes: tuple[str, ...],
    ) -> str:
        lines = [
            "Источники: " + ", ".join(source.label for source in source_refs) + ".",
            provider_text.strip(),
        ]
        lines.extend(notes)
        answer = "\n".join(line for line in lines if line)
        if len(answer) <= self.config.ai_max_answer_chars:
            return answer
        return (
            answer[: self.config.ai_max_answer_chars - 30].rstrip()
            + "\n\nОтвет обрезан по лимиту Telegram."
        )

    def _build_notes(
        self,
        *,
        source_count: int,
        truncation_count: int,
        rows_selected: int,
    ) -> tuple[str, ...]:
        notes: list[str] = []
        if source_count > 1:
            notes.append("Ответ собран по нескольким источникам.")
        if truncation_count:
            notes.append("Часть контекста была обрезана по лимитам AI.")
        if rows_selected == 0:
            notes.append("Явно релевантных строк не найдено, ответ опирается на краткую сводку источников.")
        return tuple(notes)
