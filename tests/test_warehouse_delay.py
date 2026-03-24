from __future__ import annotations

from datetime import date

import pandas as pd

from bot.services.warehouse_delay import (
    CANONICAL_ROW_ORDER,
    SHEET_HEADERS,
    WarehouseDelayAggregationResult,
    build_warehouse_delay_sheet_rows,
    bucketize_hours,
    calculate_file_statistics,
    is_without_assignment,
    make_empty_aggregation_map,
    map_filename_to_canonical_row,
    normalize_filename,
    sum_total_row,
    aggregate_warehouse_delay_files,
)


def test_normalize_filename_ignores_extension_hyphen_and_spacing() -> None:
    assert (
        normalize_filename("Невинномысск   Б1-Красота.xlsx")
        == "невинномысск б1 красота"
    )


def test_map_filename_to_canonical_row_matches_aliases() -> None:
    assert (
        map_filename_to_canonical_row("Невинномысск Б1 - Красота.xlsx")
        == "Невинномысск Б1 - Красота"
    )
    assert (
        map_filename_to_canonical_row("Невинномысск Б1-Красота 24.03.2026.xlsx")
        == "Невинномысск Б1 - Красота"
    )
    assert (
        map_filename_to_canonical_row("Невинномысск КБ1 Электроника.xls")
        == "Невинномысск КБ1 - Электроника"
    )


def test_bucketize_hours_uses_expected_boundaries() -> None:
    assert bucketize_hours(5.99) is None
    assert bucketize_hours(6) == ">6ч"
    assert bucketize_hours(12) == ">12ч"
    assert bucketize_hours(79.99) == ">60ч"
    assert bucketize_hours(200) == ">200ч"


def test_without_assignment_filter() -> None:
    assert is_without_assignment("Задание 123") is False
    assert is_without_assignment("Задания на отбор") is False
    assert is_without_assignment("Ожидает обработки") is True
    assert is_without_assignment(None) is True


def test_calculate_file_statistics_counts_buckets_and_without_assignment() -> None:
    df = pd.DataFrame(
        {
            "МХ обработки": [
                "Задания 1",
                "В очереди",
                None,
                "Задание 2",
                "Ручная обработка",
            ],
            "Задержка, ч": [6, 12, "18:30:00", "bad", 205],
        }
    )

    row_name, all_stats, no_assignment_stats, file_stats = calculate_file_statistics(
        df, "Невинномысск Б1 - Красота.xlsx"
    )

    assert row_name == "Невинномысск Б1 - Красота"
    assert all_stats["Общее количество"] == 4
    assert all_stats[">6ч"] == 1
    assert all_stats[">12ч"] == 1
    assert all_stats[">18ч"] == 1
    assert all_stats[">200ч"] == 1
    assert no_assignment_stats["Общее количество"] == 3
    assert no_assignment_stats[">12ч"] == 1
    assert no_assignment_stats[">18ч"] == 1
    assert no_assignment_stats[">200ч"] == 1
    assert file_stats.invalid_hours_rows == 1


def test_calculate_file_statistics_can_resolve_row_from_structure() -> None:
    df = pd.DataFrame(
        {
            "Склад": ["Невинномысск Блок 2", "Невинномысск Блок 2"],
            "МХ обработки": ["В работе", "Задание 1"],
            "Часы": [30, 12],
        }
    )

    row_name, _, _, file_stats = calculate_file_statistics(df, "report.xlsx")

    assert row_name == "Невинномысск Блок 2"
    assert file_stats.matched_by == "structure"


def test_sum_total_row_sums_all_warehouses() -> None:
    rows_map = make_empty_aggregation_map()
    rows_map["Невинномысск Б1 - Красота"][">6ч"] = 2
    rows_map["Невинномысск Б1 - Красота"]["Общее количество"] = 3
    rows_map["Невинномысск КБ1"][">12ч"] = 4
    rows_map["Невинномысск КБ1"]["Общее количество"] = 4

    totals = sum_total_row(rows_map)

    assert totals[">6ч"] == 2
    assert totals[">12ч"] == 4
    assert totals["Общее количество"] == 7


def test_aggregate_skips_unrecognized_file(tmp_path) -> None:
    known_df = pd.DataFrame({"МХ обработки": ["В работе"], "Часы": [12]})
    unknown_df = pd.DataFrame({"МХ обработки": ["В работе"], "Часы": [18]})

    known_path = tmp_path / "Невинномысск Б1.xlsx"
    unknown_path = tmp_path / "mystery.xlsx"
    known_df.to_excel(known_path, index=False)
    unknown_df.to_excel(unknown_path, index=False)

    aggregation = aggregate_warehouse_delay_files(
        [
            (known_path.name, known_path),
            (unknown_path.name, unknown_path),
        ]
    )

    assert aggregation.processed_files_count == 1
    assert aggregation.skipped_files == [unknown_path.name]
    assert aggregation.all_rows["Невинномысск Б1"][">12ч"] == 1


def test_build_sheet_rows_produces_two_blocks_with_totals() -> None:
    all_rows = make_empty_aggregation_map()
    no_assignment_rows = make_empty_aggregation_map()
    all_rows["Невинномысск Б1 - Красота"][">6ч"] = 2
    all_rows["Невинномысск Б1 - Красота"]["Общее количество"] = 3
    no_assignment_rows["Невинномысск Б1 - Красота"][">6ч"] = 1
    no_assignment_rows["Невинномысск Б1 - Красота"]["Общее количество"] = 1
    aggregation = WarehouseDelayAggregationResult(
        all_rows=all_rows,
        no_assignment_rows=no_assignment_rows,
        processed_files=[],
        skipped_files=[],
    )

    rows = build_warehouse_delay_sheet_rows(aggregation, date(2026, 3, 24))

    first_total_row_index = 2 + len(CANONICAL_ROW_ORDER)
    second_block_title_index = first_total_row_index + 3

    assert rows[0] == ["24.03.2026"]
    assert rows[1] == SHEET_HEADERS
    assert rows[2][0] == CANONICAL_ROW_ORDER[0]
    assert rows[first_total_row_index][0] == "Общее количество"
    assert rows[first_total_row_index][-1] == 3
    assert rows[first_total_row_index + 1] == []
    assert rows[first_total_row_index + 2] == []
    assert rows[second_block_title_index] == ["24.03.2026 без задания"]
    assert rows[second_block_title_index + 1] == SHEET_HEADERS
