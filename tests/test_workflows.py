from __future__ import annotations

import asyncio
from pathlib import Path

import pytest

from bot.config import Config
from bot.services.workflows import (
    ProcessingBusyError,
    ProcessingService,
    PublicUrlValidationError,
    validate_public_http_url,
)


def make_config(tmp_path: Path) -> Config:
    credentials_path = tmp_path / "credentials.json"
    credentials_path.write_text("{}", encoding="utf-8")
    return Config(
        telegram_token="",
        spreadsheet_id="spreadsheet-id",
        google_credentials_path=credentials_path,
        db_path=tmp_path / "bot.db",
        yandex_oauth_token="token",
        web_secret_key="secret",
    )


def test_processing_lock_rejects_parallel_work(tmp_path: Path) -> None:
    service = ProcessingService.from_root(
        make_config(tmp_path),
        object(),
        root_dir=tmp_path,
    )
    release = service._acquire_global_lock()
    try:
        with pytest.raises(ProcessingBusyError):
            service._acquire_global_lock()
    finally:
        release()


def test_validate_public_http_url_rejects_loopback() -> None:
    with pytest.raises(PublicUrlValidationError):
        asyncio.run(validate_public_http_url("http://127.0.0.1/file.xlsx"))
