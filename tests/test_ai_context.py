from __future__ import annotations

from pathlib import Path

import pandas as pd

from bot.config import Config
from bot.services.ai.context_builder import AIContextBuilder
from bot.services.ai.types import (
    AI24hSnapshotContent,
    AIExtractionResult,
    AISourceRef,
    AIWarehouseContent,
)
from bot.services.excel_24h import SnapshotMeta
from bot.services.warehouse_delay import (
    WarehouseDelayAggregationResult,
    WarehouseDelayFileStats,
    make_empty_aggregation_map,
)


def make_config(tmp_path: Path, **overrides: object) -> Config:
    credentials_path = tmp_path / "credentials.json"
    credentials_path.write_text("{}", encoding="utf-8")
    defaults: dict[str, object] = {
        "telegram_token": "token",
        "spreadsheet_id": "spreadsheet-id",
        "google_credentials_path": credentials_path,
        "db_path": tmp_path / "bot.db",
        "ai_max_rows_per_source": 2,
        "ai_max_context_chars": 6000,
    }
    defaults.update(overrides)
    return Config(**defaults)


def test_context_builder_builds_no_move_summary(tmp_path: Path) -> None:
    builder = AIContextBuilder(make_config(tmp_path))
    dataframe = pd.DataFrame(
        {
            "Гофра": ["0", "3", "0"],
            "ШК": ["SKU-1", "SKU-2", "SKU-3"],
            "Стоимость": [2500, 5000, 1500],
            "Наименование": ["A", "B", "C"],
        }
    )
    extraction = AIExtractionResult(
        source_ref=AISourceRef(kind="uploaded_file", label="upload"),
        extracted_kind="no_move",
        display_name="upload.xlsx",
        content=dataframe,
        rows_scanned=3,
    )

    context = builder.build_context([extraction], "Покажи гофру 0")

    assert "upload.xlsx" in context.text
    assert "Гофра 0" in context.text
    assert context.total_rows_scanned == 3
    assert context.total_rows_selected >= 1


def test_context_builder_marks_truncation(tmp_path: Path) -> None:
    builder = AIContextBuilder(make_config(tmp_path, ai_max_rows_per_source=1))
    dataframe = pd.DataFrame(
        {
            "Гофра": ["0", "0"],
            "ШК": ["SKU-1", "SKU-2"],
            "Стоимость": [2500, 2400],
        }
    )
    extraction = AIExtractionResult(
        source_ref=AISourceRef(kind="uploaded_file", label="upload"),
        extracted_kind="no_move",
        display_name="upload.xlsx",
        content=dataframe,
        rows_scanned=2,
    )

    context = builder.build_context([extraction], "Покажи гофру 0")

    assert context.truncation_count >= 1
    assert "обрезана" in context.text


def test_context_builder_supports_h24_and_warehouse_delay(tmp_path: Path) -> None:
    builder = AIContextBuilder(make_config(tmp_path))
    h24 = AIExtractionResult(
        source_ref=AISourceRef(kind="project_24h", label="24h"),
        extracted_kind="h24",
        display_name="24h snapshot",
        content=AI24hSnapshotContent(
            snapshot={"P1": {"forecast": "2026-04-07T10:00:00", "cost": 1000, "tare_id": "T1"}},
            meta=SnapshotMeta(
                uploaded_at="2026-04-07T09:00:00",
                source_filename="24h.xlsx",
                rows_total=1,
                rows_after_filter=1,
                rows_valid=1,
                dropped_missing=0,
                dropped_forecast=0,
                dropped_block=0,
            ),
        ),
        rows_scanned=1,
    )
    aggregation = WarehouseDelayAggregationResult(
        all_rows=make_empty_aggregation_map(),
        no_assignment_rows=make_empty_aggregation_map(),
        processed_files=[WarehouseDelayFileStats("a.xlsx", "Невинномысск", 4, 1, 0, "filename")],
        skipped_files=[],
    )
    aggregation.all_rows["Невинномысск"]["Общее количество"] = 4
    warehouse = AIExtractionResult(
        source_ref=AISourceRef(kind="yadisk_warehouse_latest", label="warehouse"),
        extracted_kind="warehouse_delay",
        display_name="warehouse.xlsx",
        content=AIWarehouseContent(aggregation=aggregation),
        rows_scanned=4,
    )

    context = builder.build_context([h24, warehouse], "Что по 24ч и задержке?")

    assert "24h snapshot" in context.text
    assert "warehouse.xlsx" in context.text
