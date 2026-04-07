from __future__ import annotations

import asyncio
import logging
import time

from bot.config import Config
from bot.services.ai.provider import (
    AIProviderConfigurationError,
    AIProviderTemporaryError,
)
from bot.services.ai.types import AIProviderResult, LLMMessage

logger = logging.getLogger(__name__)


class OpenAIProvider:
    def __init__(self, config: Config) -> None:
        self.config = config
        self._client = None

    def _get_openai_exports(self):
        try:
            from openai import (
                APIConnectionError,
                APIStatusError,
                APITimeoutError,
                AsyncOpenAI,
                RateLimitError,
            )
        except ImportError as exc:
            raise AIProviderConfigurationError(
                "Пакет openai не установлен. Установите зависимости проекта."
            ) from exc
        return AsyncOpenAI, APITimeoutError, APIConnectionError, RateLimitError, APIStatusError

    def _try_get_openai_error_types(self):
        try:
            _, APITimeoutError, APIConnectionError, RateLimitError, APIStatusError = (
                self._get_openai_exports()
            )
        except AIProviderConfigurationError:
            return None
        return (APITimeoutError, APIConnectionError, RateLimitError, APIStatusError)

    def _get_client(self):
        if not self.config.openai_api_key:
            raise AIProviderConfigurationError("OPENAI_API_KEY не настроен.")
        if not self.config.openai_model:
            raise AIProviderConfigurationError("OPENAI_MODEL не настроен.")
        if self._client is None:
            AsyncOpenAI, *_ = self._get_openai_exports()
            self._client = AsyncOpenAI(
                api_key=self.config.openai_api_key,
                base_url=self.config.openai_base_url,
                timeout=self.config.openai_timeout_seconds,
            )
        return self._client

    async def complete(
        self,
        *,
        messages: list[LLMMessage],
        max_output_tokens: int,
    ) -> AIProviderResult:
        client = self._get_client()
        error_types = self._try_get_openai_error_types()
        started = time.perf_counter()
        attempts = max(1, self.config.ai_max_retries + 1)
        last_error: Exception | None = None

        for attempt in range(1, attempts + 1):
            try:
                response = await client.responses.create(
                    model=self.config.openai_model,
                    input=[
                        {
                            "role": message.role,
                            "content": [{"type": "input_text", "text": message.content}],
                        }
                        for message in messages
                    ],
                    temperature=self.config.ai_temperature,
                    max_output_tokens=max(32, max_output_tokens),
                )
                text = (getattr(response, "output_text", "") or "").strip()
                if not text:
                    raise AIProviderTemporaryError("OpenAI вернул пустой ответ.")
                latency_ms = int((time.perf_counter() - started) * 1000)
                return AIProviderResult(
                    text=text,
                    model=self.config.openai_model or "unknown",
                    attempt_count=attempt,
                    latency_ms=latency_ms,
                )
            except AIProviderConfigurationError:
                raise
            except Exception as exc:
                if error_types is not None and isinstance(exc, error_types[:3]):
                    last_error = exc
                    logger.warning(
                        "AI temporary OpenAI error attempt=%s error=%s",
                        attempt,
                        type(exc).__name__,
                    )
                elif (
                    error_types is not None
                    and isinstance(exc, error_types[3])
                    and getattr(exc, "status_code", 0) < 500
                ):
                    raise AIProviderTemporaryError(
                        f"OpenAI вернул ошибку {getattr(exc, 'status_code', 'unknown')}."
                    ) from exc
                elif (
                    error_types is not None
                    and isinstance(exc, error_types[3])
                    and getattr(exc, "status_code", 0) >= 500
                ):
                    last_error = exc
                    logger.warning(
                        "AI OpenAI status error attempt=%s status=%s",
                        attempt,
                        getattr(exc, "status_code", "unknown"),
                    )
                elif isinstance(exc, AIProviderTemporaryError):
                    last_error = exc
                    logger.warning(
                        "AI temporary OpenAI error attempt=%s error=%s",
                        attempt,
                        type(exc).__name__,
                    )
                else:
                    raise
            if attempt >= attempts:
                break
            await asyncio.sleep(self.config.ai_retry_backoff_ms / 1000)

        raise AIProviderTemporaryError(
            "OpenAI временно недоступен. Попробуйте позже."
        ) from last_error
