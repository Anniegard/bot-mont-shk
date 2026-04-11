from __future__ import annotations

from typing import Protocol

from bot.services.ai.types import AIProviderResult, LLMMessage


class AIProviderError(Exception):
    pass


class AIProviderConfigurationError(AIProviderError):
    pass


class AIProviderTemporaryError(AIProviderError):
    pass


class LLMClient(Protocol):
    async def complete(
        self,
        *,
        messages: list[LLMMessage],
        max_output_tokens: int,
    ) -> AIProviderResult: ...
