from __future__ import annotations

import asyncio
from pathlib import Path
from types import SimpleNamespace

import pytest

from bot.config import Config
from bot.services.ai.openai_provider import OpenAIProvider
from bot.services.ai.provider import (
    AIProviderConfigurationError,
    AIProviderTemporaryError,
)
from bot.services.ai.types import LLMMessage


def make_config(tmp_path: Path, **overrides: object) -> Config:
    credentials_path = tmp_path / "credentials.json"
    credentials_path.write_text("{}", encoding="utf-8")
    defaults: dict[str, object] = {
        "telegram_token": "token",
        "spreadsheet_id": "spreadsheet-id",
        "google_credentials_path": credentials_path,
        "db_path": tmp_path / "bot.db",
        "ai_enabled": True,
        "openai_api_key": "key",
        "openai_model": "gpt-4.1-mini",
    }
    defaults.update(overrides)
    return Config(**defaults)


def test_openai_provider_requires_api_key(tmp_path: Path) -> None:
    provider = OpenAIProvider(make_config(tmp_path, openai_api_key=None))

    with pytest.raises(AIProviderConfigurationError, match="OPENAI_API_KEY"):
        asyncio.run(
            provider.complete(
                messages=[LLMMessage(role="user", content="test")],
                max_output_tokens=100,
            )
        )


def test_openai_provider_returns_output_text(tmp_path: Path) -> None:
    provider = OpenAIProvider(make_config(tmp_path))

    class FakeResponses:
        async def create(self, **_kwargs):
            return SimpleNamespace(output_text="Готовый ответ")

    provider._client = SimpleNamespace(responses=FakeResponses())  # type: ignore[assignment]

    result = asyncio.run(
        provider.complete(
            messages=[LLMMessage(role="user", content="test")],
            max_output_tokens=100,
        )
    )

    assert result.text == "Готовый ответ"
    assert result.model == "gpt-4.1-mini"
    assert result.attempt_count == 1
    assert result.latency_ms >= 0


def test_openai_provider_raises_temporary_error_on_empty_output(tmp_path: Path) -> None:
    provider = OpenAIProvider(make_config(tmp_path, ai_max_retries=0))

    class FakeResponses:
        async def create(self, **_kwargs):
            return SimpleNamespace(output_text="")

    provider._client = SimpleNamespace(responses=FakeResponses())  # type: ignore[assignment]

    with pytest.raises(AIProviderTemporaryError, match="OpenAI временно недоступен"):
        asyncio.run(
            provider.complete(
                messages=[LLMMessage(role="user", content="test")],
                max_output_tokens=100,
            )
        )
