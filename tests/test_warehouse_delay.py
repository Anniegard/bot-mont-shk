from __future__ import annotations

from datetime import date

import gspread
import pandas as pd
import pytest

from bot.services.sheets import update_warehouse_delay_sheet
from bot.services.warehouse_delay import (
    CANONICAL_ROW_ORDER,
    SHEET_HEADERS,
    TOP_WITHOUT_ASSIGNMENT_HEADERS,
    TOP_WITHOUT_ASSIGNMENT_TITLE,
    WarehouseDelayAggregationResult,
    WarehouseDelayTopItem,
    build_warehouse_delay_sheet_matrix,
    build_warehouse_delay_sheet_rows,
    bucketize_hours,
    calculate_file_statistics,
    is_without_assignment,
    make_empty_aggregation_map,
    map_filename_to_canonical_row,
    normalize_filename,
    parse_delay_hours,
    process_warehouse_delay_consolidated_file,
    resolve_columns,
    sum_total_row,
    aggregate_warehouse_delay_files,
)


class FakeWarehouseDelayWorksheet:
    def __init__(
        self,
        values: list[list[str]],
        *,
        title: str = "Выгрузка задержка склада",
        row_count: int = 100,
        col_count: int = 16,
    ) -> None:
        self._values = [list(row) for row in values]
        self.title = title
        self.row_count = row_count
        self.col_count = col_count
        self.batch_clear_calls: list[list[str]] = []
        self.update_calls: list[tuple[str, list[list[str]], str | None]] = []
        self.resize_calls: list[tuple[int, int]] = []
        self.clear_calls = 0

    def get_all_values(self) -> list[list[str]]:
        return [list(row) for row in self._values]

    def batch_clear(self, ranges: list[str]) -> None:
        self.batch_clear_calls.append(ranges)

    def update(
        self,
        cell_range: str,
        values: list[list[str]],
        value_input_option: str | None = None,
    ) -> None:
        self.update_calls.append((cell_range, values, value_input_option))

    def resize(self, rows: int, cols: int) -> None:
        self.resize_calls.append((rows, cols))
        self.row_count = rows
        self.col_count = cols

    def clear(self) -> None:
        self.clear_calls += 1


class FakeWarehouseDelaySpreadsheet:
    def __init__(self, worksheet: FakeWarehouseDelayWorksheet) -> None:
        self.sheet1 = worksheet
        self._worksheets = {worksheet.title: worksheet}
        self.add_worksheet_calls: list[tuple[str, int, int]] = []
        self.delete_worksheet_calls: list[FakeWarehouseDelayWorksheet] = []

    def worksheet(self, name: str) -> FakeWarehouseDelayWorksheet:
        if name not in self._worksheets:
            raise gspread.WorksheetNotFound(name)
        return self._worksheets[name]

    def add_worksheet(
        self, title: str, rows: int, cols: int
    ) -> FakeWarehouseDelayWorksheet:
        worksheet = FakeWarehouseDelayWorksheet(
            [[]],
            title=title,
            row_count=rows,
            col_count=cols,
        )
        self._worksheets[title] = worksheet
        self.add_worksheet_calls.append((title, rows, cols))
        return worksheet

    def delete_worksheet(self, worksheet: FakeWarehouseDelayWorksheet) -> None:
        self.delete_worksheet_calls.append(worksheet)


class FakeWarehouseDelayClient:
    def __init__(self, spreadsheet: FakeWarehouseDelaySpreadsheet) -> None:
        self._spreadsheet = spreadsheet

    def open_by_key(self, spreadsheet_id: str) -> FakeWarehouseDelaySpreadsheet:
        return self._spreadsheet


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


def test_resolve_columns_supports_vremya_prostoya_alias() -> None:
    df = pd.DataFrame(columns=["Время простоя", "МХ обработки", "Склад"])

    mapping = resolve_columns(df)

    assert mapping.hours == "Время простоя"
    assert mapping.mx_processing == "МХ обработки"
    assert mapping.warehouse == "Склад"


@pytest.mark.parametrize(
    ("raw_value", "expected_hours"),
    [
        ("45:57", 45 + 57 / 60),
        ("46:42", 46 + 42 / 60),
        ("50:4", 50 + 4 / 60),
        ("16:33", 16 + 33 / 60),
        ("86:42", 86 + 42 / 60),
        ("5:45", 5 + 45 / 60),
        ("12:30:00", 12.5),
    ],
)
def test_parse_delay_hours_supports_hh_mm_and_hh_mm_ss_formats(
    raw_value: str, expected_hours: float
) -> None:
    assert parse_delay_hours(raw_value) == pytest.approx(expected_hours)


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


def test_process_consolidated_file_groups_rows_by_block(tmp_path) -> None:
    path = tmp_path / "warehouse_delay_single.xlsx"
    pd.DataFrame(
        {
            "Блок": [
                "Невинномысск Б1 - Красота",
                "Невинномысск Б1 - Красота",
                "Невинномысск КБ1",
                "Неизвестный блок",
            ],
            "Тара": ["T-1", "T-2", "T-3", "T-4"],
            "Время простоя (чч:мм)": ["45:57", "16:33", "86:42", "12:10"],
            "МХ обработки": ["Задание 1", "Ожидает", "Ручная обработка", "Ожидает"],
            "Кол-во неразложенного товара": [5, 2, 1, 9],
        }
    ).to_excel(path, index=False)

    aggregation = process_warehouse_delay_consolidated_file(path)

    assert aggregation.all_rows["Невинномысск Б1 - Красота"][">42ч"] == 1
    assert aggregation.all_rows["Невинномысск Б1 - Красота"][">12ч"] == 1
    assert aggregation.all_rows["Невинномысск КБ1"][">80ч"] == 1
    assert aggregation.no_assignment_rows["Невинномысск Б1 - Красота"][">12ч"] == 1
    assert aggregation.no_assignment_rows["Невинномысск КБ1"][">80ч"] == 1
    assert aggregation.processed_files[0].skipped_unknown_rows == 1


def test_process_consolidated_file_raises_for_unrecognized_blocks_only(tmp_path) -> None:
    path = tmp_path / "unknown_blocks.xlsx"
    pd.DataFrame(
        {
            "Блок": ["Совсем другой склад"],
            "Тара": ["T-1"],
            "Время простоя (чч:мм)": ["45:57"],
            "МХ обработки": ["Ожидает"],
        }
    ).to_excel(path, index=False)

    with pytest.raises(Exception, match="Не удалось распознать сводный файл"):
        process_warehouse_delay_consolidated_file(path)


def test_top_without_assignment_sorts_deduplicates_and_normalizes_quantity(tmp_path) -> None:
    path = tmp_path / "top.xlsx"
    pd.DataFrame(
        {
            "Блок": [
                "Невинномысск Б1 - Красота",
                "Невинномысск Б1 - Красота",
                "Невинномысск КБ1",
                "Невинномысск КБ1",
            ],
            "Тара": ["T-1", "T-1", "T-2", "T-3"],
            "Время простоя (чч:мм)": ["45:57", "12:00", "bad", "123:07"],
            "МХ обработки": ["Ожидает", "Ожидает", "Ожидает", "Ручная обработка"],
            "Кол-во неразложенного товара": ["", 4, 7, None],
        }
    ).to_excel(path, index=False)

    aggregation = process_warehouse_delay_consolidated_file(path)

    assert [item.tare for item in aggregation.top_without_assignment] == ["T-3", "T-1"]
    assert aggregation.top_without_assignment[0].delay_display == "123:07"
    assert aggregation.top_without_assignment[0].unplaced_quantity == 0
    assert aggregation.top_without_assignment[1].delay_display == "45:57"
    assert aggregation.top_without_assignment[1].unplaced_quantity == 0


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


def test_build_sheet_matrix_places_top_block_to_the_right() -> None:
    aggregation = WarehouseDelayAggregationResult(
        all_rows=make_empty_aggregation_map(),
        no_assignment_rows=make_empty_aggregation_map(),
        processed_files=[],
        skipped_files=[],
        top_without_assignment=[
            WarehouseDelayTopItem(
                tare="T-1",
                delay_hours=45.95,
                delay_display="45:57",
                mx_processing="Ожидает",
                unplaced_quantity=2,
            )
        ],
    )

    rows = build_warehouse_delay_sheet_matrix(aggregation, date(2026, 3, 24))
    left_width = len(SHEET_HEADERS)

    assert rows[0][0] == "24.03.2026"
    assert rows[0][left_width + 2] == TOP_WITHOUT_ASSIGNMENT_TITLE
    assert rows[1][left_width + 2 : left_width + 6] == TOP_WITHOUT_ASSIGNMENT_HEADERS
    assert rows[2][left_width + 2 : left_width + 6] == ["T-1", "45:57", "Ожидает", 2]


def test_update_warehouse_delay_sheet_preserves_worksheet_and_clears_only_values() -> None:
    worksheet = FakeWarehouseDelayWorksheet(
        [
            ["old title", "old subtitle", "old extra"],
            ["old header", "bucket", "total"],
            ["warehouse", "7", "7"],
        ]
    )
    spreadsheet = FakeWarehouseDelaySpreadsheet(worksheet)
    client = FakeWarehouseDelayClient(spreadsheet)

    update_warehouse_delay_sheet(
        client=client,
        spreadsheet_id="spreadsheet-id",
        worksheet_name=worksheet.title,
        rows=[
            ["24.03.2026"],
            ["Склад", "Общее количество"],
        ],
    )

    assert worksheet.batch_clear_calls == [["A1:C3"]]
    assert worksheet.update_calls == [
        (
            "A1:B2",
            [["24.03.2026"], ["Склад", "Общее количество"]],
            "USER_ENTERED",
        )
    ]
    assert worksheet.clear_calls == 0
    assert spreadsheet.add_worksheet_calls == []
    assert spreadsheet.delete_worksheet_calls == []
