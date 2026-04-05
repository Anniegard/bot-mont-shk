from __future__ import annotations

import asyncio
from pathlib import Path
from types import SimpleNamespace

import pandas as pd
import pytest

from bot.config import Config, load_config
from bot.handlers import (
    AI_SESSION_ACTIVE_KEY,
    AI_SESSION_HISTORY_KEY,
    BUTTON_AI_ASSISTANT,
    BotHandlers,
)
from bot.services.ai_assistant import AIAssistantResponse, AIAssistantService
from bot.services.ollama_client import OllamaClient, OllamaError
from bot.services.processing import ProcessingService


class FakeStatusMessage:
    def __init__(self) -> None:
        self.edit_calls: list[tuple[str, dict[str, object]]] = []

    async def edit_text(self, text: str, **kwargs) -> None:
        self.edit_calls.append((text, kwargs))


class FakeTelegramMessage:
    def __init__(self, text: str = "") -> None:
        self.text = text
        self.reply_calls: list[tuple[str, dict[str, object]]] = []
        self.status_messages: list[FakeStatusMessage] = []

    async def reply_text(self, text: str, **kwargs) -> FakeStatusMessage:
        self.reply_calls.append((text, kwargs))
        status_message = FakeStatusMessage()
        self.status_messages.append(status_message)
        return status_message


class FakeUpdate:
    def __init__(self, user_id: int, *, text: str = "", username: str = "tester") -> None:
        self.effective_user = SimpleNamespace(id=user_id, username=username)
        self.message = FakeTelegramMessage(text)
        self.effective_message = self.message


def make_config(tmp_path: Path, **overrides: object) -> Config:
    defaults: dict[str, object] = {
        "telegram_token": "token",
        "spreadsheet_id": "spreadsheet-id",
        "google_credentials_path": tmp_path / "credentials.json",
        "db_path": tmp_path / "bot.db",
        "admin_user_ids": ("42",),
        "yandex_oauth_token": "yandex-token",
        "yandex_no_move_dir": "disk:/BOT_UPLOADS/no_move/",
        "ollama_base_url": "http://127.0.0.1:11434",
        "ollama_model": "llama3.1:8b",
    }
    defaults.update(overrides)
    return Config(**defaults)


def make_processing_service(tmp_path: Path, config: Config) -> ProcessingService:
    return ProcessingService(
        config=config,
        gspread_client=object(),
        workdir=tmp_path,
        block_ids_path=tmp_path / "block_ids.txt",
        snapshot_path=tmp_path / "snapshot.json",
        snapshot_meta_path=tmp_path / "snapshot_meta.json",
        no_move_map_path=tmp_path,
    )


def test_load_config_reads_ai_assistant_settings(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    credentials_path = tmp_path / "credentials.json"
    credentials_path.write_text("{}", encoding="utf-8")
    env_file = tmp_path / "missing.env"

    monkeypatch.setenv("SPREADSHEET_ID", "spreadsheet-id")
    monkeypatch.setenv("GOOGLE_CREDENTIALS_PATH", str(credentials_path))
    monkeypatch.setenv("OLLAMA_BASE_URL", "http://127.0.0.1:11434")
    monkeypatch.setenv("OLLAMA_MODEL", "mistral")
    monkeypatch.setenv("OLLAMA_TIMEOUT_SECONDS", "90")
    monkeypatch.setenv("AI_ASSISTANT_MAX_HISTORY_MESSAGES", "6")
    monkeypatch.setenv("AI_ASSISTANT_MAX_CONTEXT_ROWS", "40")
    monkeypatch.setenv("AI_ASSISTANT_MAX_CONTEXT_CHARS", "9000")
    monkeypatch.setenv("AI_ASSISTANT_MAX_REPLY_CHARS", "2800")

    config = load_config(env_path=str(env_file), require_telegram_token=False)

    assert config.ollama_base_url == "http://127.0.0.1:11434"
    assert config.ollama_model == "mistral"
    assert config.ollama_timeout_seconds == 90
    assert config.ai_assistant_max_history_messages == 6
    assert config.ai_assistant_max_context_rows == 40
    assert config.ai_assistant_max_context_chars == 9000
    assert config.ai_assistant_max_reply_chars == 2800


def test_reply_keyboard_shows_ai_button_only_for_admin(tmp_path: Path) -> None:
    handlers = BotHandlers(make_config(tmp_path), object())

    admin_keyboard = handlers._reply_keyboard_for_user(42)
    user_keyboard = handlers._reply_keyboard_for_user(7)

    admin_buttons = [button.text for row in admin_keyboard.keyboard for button in row]
    user_buttons = [button.text for row in user_keyboard.keyboard for button in row]

    assert BUTTON_AI_ASSISTANT in admin_buttons
    assert BUTTON_AI_ASSISTANT not in user_buttons


def test_handle_text_routes_ai_session_to_service(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    handlers = BotHandlers(make_config(tmp_path), object())
    update = FakeUpdate(42, text="Покажи товары по 0 таре")
    context = SimpleNamespace(
        user_data={
            AI_SESSION_ACTIVE_KEY: True,
            AI_SESSION_HISTORY_KEY: [],
        }
    )

    async def fake_answer_question(*, question: str, history):
        assert question == "Покажи товары по 0 таре"
        assert history == []
        return AIAssistantResponse(
            text="Файл: latest.xlsx.\nСовпавших строк: 1.\nНайден один товар.",
            source_name="latest.xlsx",
            source_path="disk:/BOT_UPLOADS/no_move/latest.xlsx",
            matched_rows=1,
            applied_filters=("гофра = 0",),
        )

    monkeypatch.setattr(handlers.ai_assistant_service, "answer_question", fake_answer_question)

    asyncio.run(handlers.handle_text(update, context))

    assert update.message.reply_calls[0][0].startswith("Смотрю последний файл")
    assert (
        update.message.status_messages[0].edit_calls[0][0]
        == "Файл: latest.xlsx.\nСовпавших строк: 1.\nНайден один товар."
    )
    assert context.user_data[AI_SESSION_HISTORY_KEY] == [
        {"role": "user", "content": "Покажи товары по 0 таре"},
        {
            "role": "assistant",
            "content": "Файл: latest.xlsx.\nСовпавших строк: 1.\nНайден один товар.",
        },
    ]


def test_enter_ai_assistant_requires_admin(tmp_path: Path) -> None:
    handlers = BotHandlers(make_config(tmp_path), object())
    update = FakeUpdate(7, text=BUTTON_AI_ASSISTANT)
    context = SimpleNamespace(user_data={})

    asyncio.run(handlers.enter_ai_assistant(update, context))

    assert update.message.reply_calls[0][0] == "У вас нет прав для этой команды."
    assert context.user_data.get(AI_SESSION_ACTIVE_KEY) is None


def test_ai_assistant_service_answer_question_uses_latest_no_move_file(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    config = make_config(
        tmp_path,
        ai_assistant_max_context_rows=20,
        ai_assistant_max_context_chars=6000,
    )
    service = AIAssistantService(
        config=config,
        processing_service=make_processing_service(tmp_path, config),
    )
    captured_messages: dict[str, object] = {}

    async def fake_yadisk_list_latest(token: str, folder_path: str, exts) -> dict[str, object]:
        assert token == "yandex-token"
        assert folder_path == "disk:/BOT_UPLOADS/no_move/"
        return {
            "name": "latest.xlsx",
            "path": "disk:/BOT_UPLOADS/no_move/latest.xlsx",
            "modified": "2026-04-05T12:00:00+03:00",
        }

    async def fake_yadisk_download_file(
        token: str, file_path: str, dest_path: str, max_bytes: int = 0
    ) -> dict[str, object]:
        Path(dest_path).write_text("placeholder", encoding="utf-8")
        return {"path": dest_path, "size": 12}

    async def fake_chat(messages) -> str:
        captured_messages["messages"] = messages
        return "Найдена одна строка по гофре 0 со стоимостью выше 2000."

    monkeypatch.setattr(
        "bot.services.ai_assistant.yadisk_list_latest", fake_yadisk_list_latest
    )
    monkeypatch.setattr(
        "bot.services.ai_assistant.yadisk_download_file", fake_yadisk_download_file
    )
    monkeypatch.setattr(
        "bot.services.ai_assistant.maybe_extract_zip",
        lambda file_path, _: file_path,
    )
    monkeypatch.setattr(
        "bot.services.ai_assistant.load_no_move_dataframe",
        lambda _: pd.DataFrame(
            {
                "Гофра": ["0", "3", "0"],
                "ШК": ["SKU-1", "SKU-2", "SKU-3"],
                "Стоимость": [2500, 5000, 1800],
                "Наименование": ["Товар A", "Товар B", "Товар C"],
            }
        ),
    )
    monkeypatch.setattr(service.ollama_client, "chat", fake_chat)

    response = asyncio.run(
        service.answer_question(
            question="Дай список товаров с 0 тарой и стоимостью больше 2000"
        )
    )

    assert response.source_name == "latest.xlsx"
    assert response.matched_rows == 1
    assert "гофра = 0" in response.applied_filters
    assert "стоимость > 2000" in response.applied_filters
    prompt_text = captured_messages["messages"][-1].content
    assert "SKU-1" in prompt_text
    assert "SKU-2" not in prompt_text
    assert "Нестандартные гофры" in prompt_text


def test_ai_assistant_service_requires_ollama_model(tmp_path: Path) -> None:
    config = make_config(tmp_path, ollama_model=None)
    service = AIAssistantService(
        config=config,
        processing_service=make_processing_service(tmp_path, config),
    )

    with pytest.raises(OllamaError, match="OLLAMA_MODEL"):
        service.ensure_configured()


def test_ollama_client_rejects_non_local_base_url() -> None:
    with pytest.raises(OllamaError, match="локальный Ollama"):
        OllamaClient(
            base_url="https://example.com",
            model="llama3.1:8b",
            timeout_seconds=30,
        )
