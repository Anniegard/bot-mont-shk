from __future__ import annotations

import asyncio
from pathlib import Path
from types import SimpleNamespace

import pytest

from bot.config import Config
from bot.handlers import BotHandlers


class FakeStatusMessage:
    def __init__(self) -> None:
        self.edit_calls: list[tuple[str, dict[str, object]]] = []

    async def edit_text(self, text: str, **kwargs) -> None:
        self.edit_calls.append((text, kwargs))


class FakeTelegramFile:
    def __init__(self, path: Path) -> None:
        self.path = path

    async def download_to_drive(self, custom_path: str) -> str:
        Path(custom_path).write_bytes(self.path.read_bytes())
        return custom_path


class FakeTelegramDocument:
    def __init__(self, path: Path, *, size: int | None = None) -> None:
        self._path = path
        self.file_name = path.name
        self.file_size = size if size is not None else path.stat().st_size
        self.file_unique_id = "file-unique"
        self.file_id = "file-id"

    async def get_file(self) -> FakeTelegramFile:
        return FakeTelegramFile(self._path)


class FakeTelegramMessage:
    def __init__(self, text: str = "", document: FakeTelegramDocument | None = None) -> None:
        self.text = text
        self.document = document
        self.reply_calls: list[tuple[str, dict[str, object]]] = []
        self.status_messages: list[FakeStatusMessage] = []

    async def reply_text(self, text: str, **kwargs) -> FakeStatusMessage:
        self.reply_calls.append((text, kwargs))
        status = FakeStatusMessage()
        self.status_messages.append(status)
        return status


class FakeUpdate:
    def __init__(
        self,
        user_id: int,
        *,
        text: str = "",
        document: FakeTelegramDocument | None = None,
        username: str = "tester",
    ) -> None:
        self.effective_user = SimpleNamespace(id=user_id, username=username)
        self.effective_message = FakeTelegramMessage(text=text, document=document)
        self.message = self.effective_message


def make_config(tmp_path: Path, **overrides: object) -> Config:
    credentials_path = tmp_path / "credentials.json"
    credentials_path.write_text("{}", encoding="utf-8")
    defaults: dict[str, object] = {
        "telegram_token": "token",
        "spreadsheet_id": "spreadsheet-id",
        "google_credentials_path": credentials_path,
        "db_path": tmp_path / "bot.db",
        "admin_user_ids": ("42",),
        "ai_admin_user_ids": ("42",),
        "ai_enabled": True,
        "ai_provider": "openai",
        "openai_api_key": "key",
        "openai_model": "gpt-4.1-mini",
    }
    defaults.update(overrides)
    return Config(**defaults)


def test_ai_enter_requires_admin(tmp_path: Path) -> None:
    handlers = BotHandlers(make_config(tmp_path), object())
    update = FakeUpdate(7, text="/ai")
    context = SimpleNamespace(user_data={}, args=[])

    asyncio.run(handlers.ai_enter(update, context))

    assert update.effective_message.reply_calls[0][0] == "У вас нет прав для этой команды."


def test_ai_use_and_text_routes_to_controller(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    handlers = BotHandlers(make_config(tmp_path), object())
    context = SimpleNamespace(user_data={}, args=["no_move"])
    enter_update = FakeUpdate(42, text="/ai")
    asyncio.run(handlers.ai_enter(enter_update, context))

    use_update = FakeUpdate(42, text="/ai_use")
    asyncio.run(handlers.ai_use(use_update, context))
    assert "Источник добавлен" in use_update.effective_message.reply_calls[0][0]

    async def fake_answer_question(**_kwargs):
        from bot.services.ai.types import AIAssistantResult

        return AIAssistantResult(
            answer_text="Источники: Последний no_move с Я.Диска.\nОтвет.",
            model="gpt-4.1-mini",
            source_labels=("Последний no_move с Я.Диска",),
            total_rows_scanned=10,
            total_rows_selected=2,
            context_chars=300,
            truncation_count=0,
            provider_attempt_count=1,
            provider_latency_ms=10,
        )

    monkeypatch.setattr(handlers.telegram_ai.ai_service, "answer_question", fake_answer_question)

    question_update = FakeUpdate(42, text="Что в no_move?")
    asyncio.run(handlers.handle_text(question_update, context))

    assert question_update.effective_message.status_messages[0].edit_calls[0][0].startswith("Источники:")


def test_ai_file_upload_is_separate_from_processing_workflow(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    handlers = BotHandlers(make_config(tmp_path), object())
    context = SimpleNamespace(user_data={}, args=[])
    enter_update = FakeUpdate(42, text="/ai")
    asyncio.run(handlers.ai_enter(enter_update, context))

    called = {"processing": False}

    async def fake_process_local_source(*_args, **_kwargs):
        called["processing"] = True
        raise AssertionError("regular processing must not run in AI mode")

    monkeypatch.setattr(handlers.processing_service, "process_local_source", fake_process_local_source)

    file_path = tmp_path / "data.csv"
    file_path.write_text("a,b\n1,2\n", encoding="utf-8")
    document = FakeTelegramDocument(file_path)
    upload_update = FakeUpdate(42, document=document)

    asyncio.run(handlers.handle_file(upload_update, context))

    assert called["processing"] is False
    assert "AI-источник сохранён" in upload_update.effective_message.reply_calls[0][0]
