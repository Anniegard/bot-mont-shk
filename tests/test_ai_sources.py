from __future__ import annotations

import asyncio
from pathlib import Path

import pandas as pd
import pytest

from bot.config import Config
from bot.services.ai.sources import AISourceError, AISourceLoader
from bot.services.ai.types import AISourceRef
from bot.services.processing import ProcessingService


def make_config(tmp_path: Path, **overrides: object) -> Config:
    credentials_path = tmp_path / "credentials.json"
    credentials_path.write_text("{}", encoding="utf-8")
    defaults: dict[str, object] = {
        "telegram_token": "token",
        "spreadsheet_id": "spreadsheet-id",
        "google_credentials_path": credentials_path,
        "db_path": tmp_path / "bot.db",
        "ai_enabled": True,
        "ai_max_file_mb": 1,
        "yandex_oauth_token": "token",
        "yandex_no_move_dir": "disk:/BOT_UPLOADS/no_move/",
        "yandex_24h_dir": "disk:/BOT_UPLOADS/24h/",
        "yandex_warehouse_delay_dir": "disk:/BOT_UPLOADS/warehouse_delay/",
    }
    defaults.update(overrides)
    return Config(**defaults)


def make_processing_service(tmp_path: Path, config: Config) -> ProcessingService:
    return ProcessingService(
        config=config,
        gspread_client=object(),
        workdir=tmp_path,
        block_ids_path=tmp_path / "block_ids.txt",
        snapshot_path=tmp_path / "last_24h_snapshot.json",
        snapshot_meta_path=tmp_path / "last_24h_meta.json",
        no_move_map_path=tmp_path,
    )


def test_source_loader_rejects_unsupported_extension(tmp_path: Path) -> None:
    config = make_config(tmp_path)
    loader = AISourceLoader(config, make_processing_service(tmp_path, config))
    path = tmp_path / "bad.txt"
    path.write_text("x", encoding="utf-8")

    with pytest.raises(AISourceError, match="Поддерживаются только"):
        asyncio.run(
            loader.load_sources(
                [
                    AISourceRef(
                        kind="uploaded_file",
                        label="bad",
                        filename="bad.txt",
                        file_path=path,
                    )
                ]
            )
        )


def test_source_loader_reads_csv_as_table(tmp_path: Path) -> None:
    config = make_config(tmp_path)
    loader = AISourceLoader(config, make_processing_service(tmp_path, config))
    path = tmp_path / "data.csv"
    pd.DataFrame({"col1": [1, 2], "col2": ["a", "b"]}).to_csv(path, index=False)

    loaded = asyncio.run(
        loader.load_sources(
            [
                AISourceRef(
                    kind="uploaded_file",
                    label="csv",
                    filename="data.csv",
                    file_path=path,
                )
            ]
        )
    )

    assert loaded.extractions[0].extracted_kind == "table"
    assert loaded.extractions[0].rows_scanned == 2
