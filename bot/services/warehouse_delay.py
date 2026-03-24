from __future__ import annotations

import logging
import re
from dataclasses import dataclass
from datetime import date, datetime, time, timedelta
from pathlib import Path
from typing import Any, Iterable

import pandas as pd

logger = logging.getLogger(__name__)

CANONICAL_ROW_ORDER = [
    "Невинномысск Б1 - Красота",
    "Невинномысск Б1 КГТ",
    "Невинномысск Б1",
    "Невинномысск КБ1 КГТ",
    "Невинномысск КБ1 - Электроника",
    "Невинномысск КБ1",
    "Невинномысск КБ2 - КГТ",
    "Невинномысск КБ2",
    "Невинномысск ПБ2П",
    "Невинномысск",
    "Невинномысск Блок 2",
    "Невинномысск ПБ2 - КГТ",
    "Невинномысск Б2 Красота",
]

BUCKETS: list[tuple[str, float, float | None]] = [
    (">6ч", 6, 12),
    (">12ч", 12, 18),
    (">18ч", 18, 24),
    (">24ч", 24, 30),
    (">30ч", 30, 36),
    (">36ч", 36, 42),
    (">42ч", 42, 48),
    (">48ч", 48, 54),
    (">54ч", 54, 60),
    (">60ч", 60, 80),
    (">80ч", 80, 100),
    (">100ч", 100, 120),
    (">120ч", 120, 200),
    (">200ч", 200, None),
]
BUCKET_LABELS = [label for label, _, _ in BUCKETS]
TOTAL_COLUMN = "Общее количество"
SHEET_HEADERS = ["Склад", *BUCKET_LABELS, TOTAL_COLUMN]

MX_PROCESSING_COLUMN_ALIASES = [
    "мх обработки",
    "мх",
    "mx обработки",
    "mx processing",
]
HOURS_COLUMN_ALIASES = [
    "часы",
    "часов",
    "задержка ч",
    "задержка, ч",
    "длительность ч",
    "длительность, ч",
    "возраст ч",
    "возраст, ч",
    "сколько часов",
    "час задержки",
    "время задержки ч",
    "время задержки, ч",
    "Время простоя",
    "время простоя",
    "Простой",
    "простой",
    "Время простоя, ч",
    "Время простоя ч",
]
WAREHOUSE_COLUMN_ALIASES = [
    "склад",
    "наименование склада",
    "склад назначения",
    "warehouse",
]


class WarehouseDelayError(Exception):
    pass


class WarehouseDelayStructureError(WarehouseDelayError):
    pass


class WarehouseDelayUnrecognizedFile(WarehouseDelayError):
    pass


@dataclass(frozen=True)
class WarehouseDelayColumnMapping:
    hours: str
    mx_processing: str
    warehouse: str | None = None


@dataclass
class WarehouseDelayFileStats:
    filename: str
    canonical_row_name: str
    processed_rows: int
    no_assignment_rows: int
    invalid_hours_rows: int
    matched_by: str


@dataclass
class WarehouseDelayAggregationResult:
    all_rows: dict[str, dict[str, int]]
    no_assignment_rows: dict[str, dict[str, int]]
    processed_files: list[WarehouseDelayFileStats]
    skipped_files: list[str]

    @property
    def processed_files_count(self) -> int:
        return len(self.processed_files)

    @property
    def skipped_files_count(self) -> int:
        return len(self.skipped_files)


def _normalize_spaces(value: str) -> str:
    return re.sub(r"\s+", " ", value).strip()


def normalize_filename(filename: str) -> str:
    stem = Path(filename).stem
    text = stem.lower().replace("ё", "е")
    text = re.sub(r"[-–—_]+", " ", text)
    text = re.sub(r"[^\w\s]", " ", text)
    return _normalize_spaces(text)


def normalize_column_name(name: str) -> str:
    text = str(name).strip().lower().replace("ё", "е")
    text = re.sub(r"[_\-–—]+", " ", text)
    text = re.sub(r"[^\w\s]", " ", text)
    return _normalize_spaces(text)


def _build_alias_map() -> dict[str, str]:
    alias_map: dict[str, str] = {}
    for canonical_name in CANONICAL_ROW_ORDER:
        alias_map[normalize_filename(canonical_name)] = canonical_name

    manual_aliases = {
        "невинномысск блок2": "Невинномысск Блок 2",
        "невинномысск блок 2": "Невинномысск Блок 2",
        "невинномысск кб2 кгт": "Невинномысск КБ2 - КГТ",
        "невинномысск пб2 кгт": "Невинномысск ПБ2 - КГТ",
        "невинномысск б2 красота": "Невинномысск Б2 Красота",
        "невинномысск б1 красота": "Невинномысск Б1 - Красота",
        "невинномысск кб1 электроника": "Невинномысск КБ1 - Электроника",
    }
    alias_map.update(manual_aliases)
    return alias_map


FILENAME_ALIAS_MAP = _build_alias_map()


def map_filename_to_canonical_row(filename: str) -> str | None:
    normalized = normalize_filename(filename)
    if not normalized:
        return None

    exact_match = FILENAME_ALIAS_MAP.get(normalized)
    if exact_match:
        return exact_match

    matches = [
        (alias, canonical_name)
        for alias, canonical_name in FILENAME_ALIAS_MAP.items()
        if re.search(rf"(^| ){re.escape(alias)}( |$)", normalized)
    ]
    if matches:
        longest_alias_length = max(len(alias) for alias, _ in matches)
        best_matches = [
            canonical_name
            for alias, canonical_name in matches
            if len(alias) == longest_alias_length
        ]
        unique_matches = list(dict.fromkeys(best_matches))
        if len(unique_matches) == 1:
            return unique_matches[0]
    return None


def _empty_row_stats() -> dict[str, int]:
    return {column: 0 for column in [*BUCKET_LABELS, TOTAL_COLUMN]}


def make_empty_aggregation_map() -> dict[str, dict[str, int]]:
    return {row_name: _empty_row_stats() for row_name in CANONICAL_ROW_ORDER}


def _find_column(columns: Iterable[str], aliases: Iterable[str]) -> str | None:
    normalized_columns = {column: normalize_column_name(column) for column in columns}
    normalized_aliases = [normalize_column_name(alias) for alias in aliases]

    for alias in normalized_aliases:
        for column, normalized_column in normalized_columns.items():
            if normalized_column == alias:
                return column

    for alias in normalized_aliases:
        for column, normalized_column in normalized_columns.items():
            if alias in normalized_column:
                return column

    return None


def resolve_columns(df: pd.DataFrame) -> WarehouseDelayColumnMapping:
    hours_column = _find_column(df.columns, HOURS_COLUMN_ALIASES)
    mx_processing_column = _find_column(df.columns, MX_PROCESSING_COLUMN_ALIASES)
    warehouse_column = _find_column(df.columns, WAREHOUSE_COLUMN_ALIASES)

    missing: list[str] = []
    if not hours_column:
        missing.append("колонка часов задержки")
    if not mx_processing_column:
        missing.append("колонка 'МХ обработки'")

    if missing:
        raise WarehouseDelayStructureError(
            "Не найдены обязательные колонки: " + ", ".join(missing)
        )

    return WarehouseDelayColumnMapping(
        hours=hours_column,
        mx_processing=mx_processing_column,
        warehouse=warehouse_column,
    )


def is_without_assignment(mx_processing_value: Any) -> bool:
    text = (
        ""
        if mx_processing_value is None or pd.isna(mx_processing_value)
        else str(mx_processing_value)
    )
    normalized = _normalize_spaces(text).lower()
    return not normalized.startswith("задание") and not normalized.startswith("задания")


def parse_delay_hours(value: Any) -> float | None:
    if value is None or pd.isna(value):
        return None

    if isinstance(value, bool):
        return None

    if isinstance(value, pd.Timedelta):
        return value.total_seconds() / 3600

    if isinstance(value, timedelta):
        return value.total_seconds() / 3600

    if isinstance(value, time):
        return (
            value.hour
            + (value.minute / 60)
            + (value.second / 3600)
            + (value.microsecond / 3_600_000_000)
        )

    if isinstance(value, datetime):
        if value.year in {1899, 1900}:
            return (
                value.hour
                + (value.minute / 60)
                + (value.second / 3600)
                + (value.microsecond / 3_600_000_000)
            )
        return None

    if isinstance(value, (int, float)):
        return float(value)

    text = str(value).strip()
    if not text:
        return None

    compact_text = text.replace(" ", "").replace(",", ".")
    if re.fullmatch(r"[+-]?\d+(?:\.\d+)?", compact_text):
        return float(compact_text)

    if re.fullmatch(r"\d{1,4}:\d{1,2}(?::\d{1,2})?", compact_text):
        parts = [int(part) for part in compact_text.split(":")]
        hours = parts[0]
        minutes = parts[1]
        seconds = parts[2] if len(parts) == 3 else 0
        return hours + (minutes / 60) + (seconds / 3600)

    if ":" in text or "day" in text.lower():
        parsed_timedelta = pd.to_timedelta(text, errors="coerce")
        if not pd.isna(parsed_timedelta):
            return parsed_timedelta.total_seconds() / 3600

    return None


def bucketize_hours(hours: float) -> str | None:
    for label, lower_bound, upper_bound in BUCKETS:
        if upper_bound is None and hours >= lower_bound:
            return label
        if upper_bound is not None and lower_bound <= hours < upper_bound:
            return label
    return None


def sum_total_row(rows_map: dict[str, dict[str, int]]) -> dict[str, int]:
    totals = _empty_row_stats()
    for row_stats in rows_map.values():
        for column in totals:
            totals[column] += row_stats.get(column, 0)
    return totals


def _resolve_row_name_from_structure(
    df: pd.DataFrame, column_mapping: WarehouseDelayColumnMapping
) -> str | None:
    if not column_mapping.warehouse:
        return None

    recognized_values: list[str] = []
    for raw_value in df[column_mapping.warehouse].dropna().tolist():
        canonical_name = map_filename_to_canonical_row(str(raw_value))
        if canonical_name:
            recognized_values.append(canonical_name)

    unique_values = list(dict.fromkeys(recognized_values))
    if len(unique_values) == 1:
        return unique_values[0]
    return None


def resolve_canonical_row_name(
    filename: str, df: pd.DataFrame, column_mapping: WarehouseDelayColumnMapping
) -> tuple[str, str]:
    canonical_from_filename = map_filename_to_canonical_row(filename)
    if canonical_from_filename:
        return canonical_from_filename, "filename"

    canonical_from_structure = _resolve_row_name_from_structure(df, column_mapping)
    if canonical_from_structure:
        return canonical_from_structure, "structure"

    raise WarehouseDelayUnrecognizedFile(
        f'Файл "{filename}" непонятен, поэтому я его не прочитал. Остальные файлы обработал.'
    )


def read_warehouse_delay_file(file_path: str | Path) -> pd.DataFrame:
    path = Path(file_path)
    read_kwargs: dict[str, str] = {}
    if path.suffix.lower() == ".xls":
        read_kwargs["engine"] = "xlrd"
    elif path.suffix.lower() in {".xlsx", ".xlsm", ".xltx", ".xltm"}:
        read_kwargs["engine"] = "openpyxl"
    try:
        return pd.read_excel(path, **read_kwargs)
    except Exception as exc:
        raise WarehouseDelayStructureError(
            f"Не удалось прочитать Excel-файл {path.name}: {exc}"
        ) from exc


def calculate_file_statistics(
    df: pd.DataFrame,
    filename: str,
) -> tuple[str, dict[str, int], dict[str, int], WarehouseDelayFileStats]:
    column_mapping = resolve_columns(df)
    canonical_row_name, matched_by = resolve_canonical_row_name(
        filename, df, column_mapping
    )

    all_stats = _empty_row_stats()
    no_assignment_stats = _empty_row_stats()
    invalid_hours_rows = 0

    for _, row in df.iterrows():
        hours = parse_delay_hours(row[column_mapping.hours])
        if hours is None:
            invalid_hours_rows += 1
            continue

        all_stats[TOTAL_COLUMN] += 1
        bucket = bucketize_hours(hours)
        if bucket:
            all_stats[bucket] += 1

        if is_without_assignment(row[column_mapping.mx_processing]):
            no_assignment_stats[TOTAL_COLUMN] += 1
            if bucket:
                no_assignment_stats[bucket] += 1

    if invalid_hours_rows:
        logger.info(
            "Warehouse delay: skipped rows with unknown hours file=%s count=%s",
            filename,
            invalid_hours_rows,
        )

    file_stats = WarehouseDelayFileStats(
        filename=filename,
        canonical_row_name=canonical_row_name,
        processed_rows=all_stats[TOTAL_COLUMN],
        no_assignment_rows=no_assignment_stats[TOTAL_COLUMN],
        invalid_hours_rows=invalid_hours_rows,
        matched_by=matched_by,
    )
    return canonical_row_name, all_stats, no_assignment_stats, file_stats


def process_warehouse_delay_file(
    file_path: str | Path, filename: str | None = None
) -> tuple[str, dict[str, int], dict[str, int], WarehouseDelayFileStats]:
    path = Path(file_path)
    dataframe = read_warehouse_delay_file(path)
    return calculate_file_statistics(dataframe, filename or path.name)


def aggregate_warehouse_delay_files(
    files: Iterable[tuple[str, str | Path]],
) -> WarehouseDelayAggregationResult:
    all_rows = make_empty_aggregation_map()
    no_assignment_rows = make_empty_aggregation_map()
    processed_files: list[WarehouseDelayFileStats] = []
    skipped_files: list[str] = []

    for filename, file_path in files:
        try:
            (
                canonical_row_name,
                file_all_stats,
                file_no_assignment_stats,
                file_stats,
            ) = process_warehouse_delay_file(file_path, filename)
        except WarehouseDelayError:
            skipped_files.append(filename)
            continue

        for column, value in file_all_stats.items():
            all_rows[canonical_row_name][column] += value
        for column, value in file_no_assignment_stats.items():
            no_assignment_rows[canonical_row_name][column] += value
        processed_files.append(file_stats)

    return WarehouseDelayAggregationResult(
        all_rows=all_rows,
        no_assignment_rows=no_assignment_rows,
        processed_files=processed_files,
        skipped_files=skipped_files,
    )


def _build_sheet_block(
    title: str, rows_map: dict[str, dict[str, int]]
) -> list[list[str | int]]:
    block_rows: list[list[str | int]] = [[title], SHEET_HEADERS]
    for row_name in CANONICAL_ROW_ORDER:
        stats = rows_map[row_name]
        block_rows.append(
            [
                row_name,
                *[stats[column] for column in BUCKET_LABELS],
                stats[TOTAL_COLUMN],
            ]
        )
    totals = sum_total_row(rows_map)
    block_rows.append(
        [
            "Общее количество",
            *[totals[column] for column in BUCKET_LABELS],
            totals[TOTAL_COLUMN],
        ]
    )
    return block_rows


def build_warehouse_delay_sheet_rows(
    aggregation: WarehouseDelayAggregationResult,
    report_date: date,
) -> list[list[str | int]]:
    formatted_date = report_date.strftime("%d.%m.%Y")
    first_block = _build_sheet_block(formatted_date, aggregation.all_rows)
    second_block = _build_sheet_block(
        f"{formatted_date} без задания",
        aggregation.no_assignment_rows,
    )
    return [*first_block, [], [], *second_block]
