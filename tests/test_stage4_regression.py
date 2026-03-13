from __future__ import annotations

from bot.constants import CASE_ID_COLUMN_NAME
from bot.db import (
    calculate_row_hash,
    insert_case_version_if_changed,
    insert_raw_sheet_row_if_new,
    insert_raw_yadisk_row_if_new,
)
from bot.services import case_sync
from bot.services.case_sync import REQUIRED_FIELD_LABELS, read_master_sheet_rows
from bot.services.yadisk_ingest import match_raw_row_to_case


def _row_count(connection, table_name: str) -> int:
    row = connection.execute(f"SELECT COUNT(*) AS count FROM {table_name}").fetchone()
    return int(row["count"])


class FakeWorksheet:
    def __init__(self, values: list[list[str]], title: str = "Master", col_count: int = 12):
        self._values = [list(row) for row in values]
        self.title = title
        self.col_count = col_count
        self.batch_update_calls: list[list[dict[str, object]]] = []
        self.update_cell_calls: list[tuple[int, int, str]] = []
        self.add_cols_calls: list[int] = []

    def row_values(self, row_number: int) -> list[str]:
        return list(self._values[row_number - 1])

    def get_all_values(self) -> list[list[str]]:
        return [list(row) for row in self._values]

    def add_cols(self, columns: int) -> None:
        self.add_cols_calls.append(columns)
        self.col_count += columns

    def update_cell(self, row: int, column: int, value: str) -> None:
        self.update_cell_calls.append((row, column, value))

    def batch_update(self, updates: list[dict[str, object]]) -> None:
        self.batch_update_calls.append(updates)


def test_case_versions_dedupe_for_same_case_and_row_hash(connection, seed_case) -> None:
    seed_case("case-1", item_name="Case 1")
    payload = {"snapshot": {"item_name": "Case 1"}}
    row_hash = calculate_row_hash(payload)

    first_id = insert_case_version_if_changed(
        case_id="case-1",
        row_hash=row_hash,
        raw_snapshot_json=payload,
        sheet_row_number=2,
        connection=connection,
    )
    second_id = insert_case_version_if_changed(
        case_id="case-1",
        row_hash=row_hash,
        raw_snapshot_json=payload,
        sheet_row_number=2,
        connection=connection,
    )

    assert first_id is not None
    assert second_id is None
    assert _row_count(connection, "case_versions") == 1


def test_raw_sheet_rows_dedupe_for_same_sheet_row_and_hash(connection) -> None:
    payload = {"case_id": "case-1", "raw": "same-row"}
    row_hash = calculate_row_hash(payload)

    first_id = insert_raw_sheet_row_if_new(
        sheet_name="master",
        row_number=2,
        row_hash=row_hash,
        raw_json=payload,
        case_id="case-1",
        connection=connection,
    )
    second_id = insert_raw_sheet_row_if_new(
        sheet_name="master",
        row_number=2,
        row_hash=row_hash,
        raw_json=payload,
        case_id="case-1",
        connection=connection,
    )

    assert first_id is not None
    assert second_id is None
    assert _row_count(connection, "raw_sheet_rows") == 1


def test_raw_yadisk_rows_dedupe_for_same_source_and_hash(connection) -> None:
    payload = {"raw_values": {"shk": "SKU-1"}}
    row_hash = calculate_row_hash(payload)

    first_id = insert_raw_yadisk_row_if_new(
        row_hash=row_hash,
        source_file_name="report.xlsx",
        source_path="telegram:file-1",
        source_kind="no_move",
        source_sheet_name="Sheet1",
        shk="SKU-1",
        normalized_json=payload,
        connection=connection,
    )
    second_id = insert_raw_yadisk_row_if_new(
        row_hash=row_hash,
        source_file_name="report.xlsx",
        source_path="telegram:file-1",
        source_kind="no_move",
        source_sheet_name="Sheet1",
        shk="SKU-1",
        normalized_json=payload,
        connection=connection,
    )

    assert first_id is not None
    assert second_id is None
    assert _row_count(connection, "raw_yadisk_rows") == 1


def test_matching_uses_shk_then_tare_transfer_then_item_name(connection, seed_case) -> None:
    seed_case("case-by-name", item_name="Target name")
    seed_case("case-by-tare", tare_transfer="TARE-1")
    seed_case("case-by-shk", shk="SHK-1")

    by_shk = match_raw_row_to_case(
        shk="SHK-1",
        tare_transfer="TARE-1",
        item_name="Target name",
        connection=connection,
    )
    by_tare = match_raw_row_to_case(
        shk="missing",
        tare_transfer="TARE-1",
        item_name="Target name",
        connection=connection,
    )
    by_name = match_raw_row_to_case(
        shk="missing",
        tare_transfer="missing",
        item_name="Target name",
        connection=connection,
    )

    assert by_shk["matched_case_id"] == "case-by-shk"
    assert by_shk["match_method"] == "shk"
    assert by_shk["match_confidence"] == "high"

    assert by_tare["matched_case_id"] == "case-by-tare"
    assert by_tare["match_method"] == "tare_transfer"
    assert by_tare["match_confidence"] == "medium"

    assert by_name["matched_case_id"] == "case-by-name"
    assert by_name["match_method"] == "item_name"
    assert by_name["match_confidence"] == "low"


def test_matching_marks_ambiguous_candidates_without_autolink(connection, seed_case) -> None:
    seed_case("case-a", shk="DUPLICATE-SHK")
    seed_case("case-b", shk="DUPLICATE-SHK")
    seed_case("fallback-case", tare_transfer="TARE-UNIQUE")

    result = match_raw_row_to_case(
        shk="DUPLICATE-SHK",
        tare_transfer="TARE-UNIQUE",
        item_name="Ignored fallback",
        connection=connection,
    )

    assert result["matched_case_id"] is None
    assert result["match_method"] == "shk"
    assert result["match_confidence"] == "ambiguous"
    assert result["link_decision_reason"] == "multiple cases found by shk"


def test_existing_case_id_is_preserved_without_regeneration(monkeypatch) -> None:
    headers = [*REQUIRED_FIELD_LABELS.values(), CASE_ID_COLUMN_NAME]
    worksheet = FakeWorksheet(
        [
            headers,
            [
                "2026-03-10",
                "Analyst",
                "Item",
                "CULPRIT-1",
                "Comment",
                "Example SHK",
                "Action",
                "Movement",
                "existing-case-id",
            ],
        ]
    )

    monkeypatch.setattr(
        case_sync,
        "get_worksheet",
        lambda client, spreadsheet_id, sheet_name: worksheet,
    )

    result = read_master_sheet_rows(
        client=object(),
        spreadsheet_id="spreadsheet-id",
        sheet_name="Master",
    )

    assert result["case_id_updates"] == 0
    assert result["rows"][0]["case_id"] == "existing-case-id"
    assert worksheet.batch_update_calls == []
    assert worksheet.update_cell_calls == []
    assert worksheet.add_cols_calls == []
