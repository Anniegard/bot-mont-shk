from __future__ import annotations

from bot.db import insert_raw_yadisk_row
from bot.services.search_service import (
    find_raw_rows_by_shk,
    get_raw_rows_for_case,
    search_cases,
    search_raw_rows,
)


def _insert_raw_row(connection, **overrides: object) -> int:
    payload = {
        "row_hash": f"stage6-raw-{overrides.get('row_hash_suffix', '1')}",
        "source_file_name": "report.xlsx",
        "source_path": "telegram:file-search",
        "source_kind": "no_move",
        "source_sheet_name": "Sheet1",
        "source_row_number": 2,
        "shk": "SHK-1",
        "tare_transfer": "TARE-1",
        "item_name": "Item 1",
        "normalized_json": {"raw": "value"},
    }
    payload.update(overrides)
    payload.pop("row_hash_suffix", None)
    return insert_raw_yadisk_row(connection=connection, **payload)


def test_search_cases_finds_exact_case_id(connection, seed_case) -> None:
    seed_case("case-42", item_name="Exact case")

    result = search_cases("case-42", connection=connection)

    assert result["match_field"] == "case_id"
    assert result["match_type"] == "exact"
    assert [row["case_id"] for row in result["results"]] == ["case-42"]


def test_search_cases_prefers_shk_before_tare_transfer(connection, seed_case) -> None:
    seed_case("case-by-tare", tare_transfer="TOKEN-1", item_name="By tare")
    seed_case("case-by-shk", shk="TOKEN-1", item_name="By shk")

    result = search_cases("TOKEN-1", connection=connection)

    assert result["match_field"] == "shk"
    assert result["match_type"] == "exact"
    assert [row["case_id"] for row in result["results"]] == ["case-by-shk"]


def test_get_raw_rows_for_case_returns_only_linked_rows_for_case(
    connection, seed_case
) -> None:
    seed_case("case-1", shk="SHK-1")
    seed_case("case-2", shk="SHK-2")
    raw_row_id = _insert_raw_row(connection, row_hash_suffix="linked-1", shk="SHK-1")
    other_row_id = _insert_raw_row(connection, row_hash_suffix="linked-2", shk="SHK-2")
    connection.execute(
        """
        UPDATE raw_yadisk_rows
        SET matched_case_id = ?, match_method = ?, match_confidence = ?, review_status = ?
        WHERE id = ?
        """,
        ("case-1", "manual", "manual", "linked", raw_row_id),
    )
    connection.execute(
        """
        UPDATE raw_yadisk_rows
        SET matched_case_id = ?, match_method = ?, match_confidence = ?, review_status = ?
        WHERE id = ?
        """,
        ("case-2", "manual", "manual", "linked", other_row_id),
    )

    rows = get_raw_rows_for_case("case-1", connection=connection)

    assert [row["id"] for row in rows] == [raw_row_id]
    assert rows[0]["matched_case_id"] == "case-1"
    assert rows[0]["review_status"] == "linked"


def test_raw_search_finds_expected_rows_by_shk_and_tare_transfer(connection) -> None:
    by_shk_id = _insert_raw_row(
        connection,
        row_hash_suffix="search-shk",
        shk="SHK-77",
        tare_transfer="OTHER-TARE",
        item_name="Search by SHK",
    )
    by_tare_id = _insert_raw_row(
        connection,
        row_hash_suffix="search-tare",
        shk="OTHER-SHK",
        tare_transfer="TARE-55",
        item_name="Search by tare",
    )

    shk_rows = find_raw_rows_by_shk("SHK-77", connection=connection)
    tare_result = search_raw_rows("TARE-55", connection=connection)

    assert [row["id"] for row in shk_rows] == [by_shk_id]
    assert tare_result["match_field"] == "tare_transfer"
    assert [row["id"] for row in tare_result["results"]] == [by_tare_id]
