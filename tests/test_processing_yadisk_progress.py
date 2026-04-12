from __future__ import annotations

import asyncio
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

from bot.config import Config
from bot.services.processing import EXPECTED_24H, ProcessingService


def _make_config(tmp_path: Path) -> Config:
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


def _make_service(tmp_path: Path) -> ProcessingService:
    data = tmp_path / "data"
    data.mkdir(parents=True, exist_ok=True)
    (tmp_path / "data" / "block_ids.txt").write_text("1\n", encoding="utf-8")
    return ProcessingService(
        _make_config(tmp_path),
        MagicMock(),
        workdir=tmp_path,
        block_ids_path=tmp_path / "data" / "block_ids.txt",
        snapshot_path=tmp_path / "data" / "snap.json",
        snapshot_meta_path=tmp_path / "data" / "snap_meta.json",
        no_move_map_path=tmp_path / "data",
    )


def test_process_latest_yadisk_file_progress_order_24h(tmp_path: Path) -> None:
    service = _make_service(tmp_path)
    calls: list[tuple[str, str, int | None]] = []

    def progress_cb(step: str, message: str, duration_ms: int | None = None) -> None:
        calls.append((step, message, duration_ms))

    latest = {
        "name": "f.xlsx",
        "path": "disk:/f.xlsx",
        "modified": "2024-01-01",
        "size": 10,
    }

    outcome_mock = MagicMock()
    outcome_mock.title = "24 часа"
    outcome_mock.message = "ok"
    outcome_mock.sheet_url = None
    outcome_mock.level = "success"
    outcome_mock.parse_mode = "HTML"
    outcome_mock.disable_web_page_preview = True
    outcome_mock.payload = {}

    async def run() -> None:
        with (
            patch(
                "bot.services.processing.yadisk_list_latest",
                new_callable=AsyncMock,
                return_value=latest,
            ),
            patch(
                "bot.services.processing.yadisk_download_file",
                new_callable=AsyncMock,
                return_value={"path": str(tmp_path / "dl.xlsx"), "size": 10},
            ),
            patch(
                "bot.services.processing.maybe_extract_zip",
                return_value=str(tmp_path / "prep.xlsx"),
            ),
            patch.object(
                service,
                "_process_prepared_source",
                new_callable=AsyncMock,
                return_value=outcome_mock,
            ),
        ):
            await service.process_latest_yadisk_file(
                EXPECTED_24H,
                progress_cb=progress_cb,
            )

    asyncio.run(run())

    steps = [c[0] for c in calls]
    assert "Папка на Я.Диске" in steps
    assert "Найден последний файл" in steps
    assert "Скачивание" in steps
    assert "Скачивание завершено" in steps
    assert "Распаковка" in steps
    assert "Распаковка завершена" in steps
    assert "Обработка файла" in steps
    assert "Обработка завершена" in steps
    assert steps.index("Папка на Я.Диске") < steps.index("Найден последний файл")
    assert steps.index("Найден последний файл") < steps.index("Скачивание завершено")


def test_process_latest_yadisk_file_works_without_progress_cb(tmp_path: Path) -> None:
    service = _make_service(tmp_path)
    latest = {
        "name": "f.xlsx",
        "path": "disk:/f.xlsx",
        "modified": "2024-01-01",
        "size": 10,
    }
    outcome_mock = MagicMock()
    outcome_mock.title = "x"
    outcome_mock.message = "ok"
    outcome_mock.sheet_url = None
    outcome_mock.level = "success"
    outcome_mock.parse_mode = "HTML"
    outcome_mock.disable_web_page_preview = True
    outcome_mock.payload = {}

    async def run() -> None:
        with (
            patch(
                "bot.services.processing.yadisk_list_latest",
                new_callable=AsyncMock,
                return_value=latest,
            ),
            patch(
                "bot.services.processing.yadisk_download_file",
                new_callable=AsyncMock,
                return_value={"path": str(tmp_path / "dl.xlsx"), "size": 10},
            ),
            patch(
                "bot.services.processing.maybe_extract_zip",
                return_value=str(tmp_path / "prep.xlsx"),
            ),
            patch.object(
                service,
                "_process_prepared_source",
                new_callable=AsyncMock,
                return_value=outcome_mock,
            ),
        ):
            await service.process_latest_yadisk_file(EXPECTED_24H)

    asyncio.run(run())
