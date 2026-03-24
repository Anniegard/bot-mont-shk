from __future__ import annotations

import logging
import re
from dataclasses import dataclass, field
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
TOP_WITHOUT_ASSIGNMENT_TITLE = "Топ 10 тар без задания"
TOP_WITHOUT_ASSIGNMENT_HEADERS = [
    "Тара",
    "Время простоя",
    "МХ обработки",
    "Кол-во неразложенного товара",
]

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
    "время простоя",
    "простой",
    "время простоя, ч",
    "время простоя ч",
    "время простоя (чч мм)",
    "время простоя (чч:мм)",
]
WAREHOUSE_COLUMN_ALIASES = [
    "склад",
    "наименование склада",
    "склад назначения",
    "warehouse",
]
BLOCK_COLUMN_ALIASES = [
    "блок",
]
TARE_COLUMN_ALIASES = [
    "тара",
    "id тары",
]
UNPLACED_QTY_COLUMN_ALIASES = [
    "кол-во неразложенного товара",
    "кол во неразложенного товара",
    "количество неразложенного товара",
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
    block: str | None = None
    tare: str | None = None
    unplaced_quantity: str | None = None


@dataclass
class WarehouseDelayFileStats:
    filename: str
    canonical_row_name: str | None
    processed_rows: int
    no_assignment_rows: int
    invalid_hours_rows: int
    matched_by: str
    skipped_unknown_rows: int = 0


@dataclass(frozen=True)
class WarehouseDelayTopItem:
    tare: str
    delay_hours: float
    delay_display: str
    mx_processing: str
    unplaced_quantity: int | float | str


@dataclass
class WarehouseDelayAggregationResult:
    all_rows: dict[str, dict[str, int]]
    no_assignment_rows: dict[str, dict[str, int]]
    processed_files: list[WarehouseDelayFileStats]
    skipped_files: list[str]
    top_without_assignment: list[WarehouseDelayTopItem] = field(default_factory=list)

    @property
    def processed_files_count(self) -> int:
        return len(self.processed_files)

    @property
    def skipped_files_count(self) -> int:
        return len(self.skipped_files)


@dataclass
class _WarehouseDelayAccumulator:
    all_rows: dict[str, dict[str, int]] = field(default_factory=lambda: make_empty_aggregation_map())
    no_assignment_rows: dict[str, dict[str, int]] = field(
        default_factory=lambda: make_empty_aggregation_map()
    )
    top_candidates: list[WarehouseDelayTopItem] = field(default_factory=list)
    processed_rows: int = 0
    no_assignment_rows_count: int = 0
    invalid_hours_rows: int = 0
    skipped_unknown_rows: int = 0


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
    tare_column = _find_column(df.columns, TARE_COLUMN_ALIASES)
    unplaced_quantity_column = _find_column(df.columns, UNPLACED_QTY_COLUMN_ALIASES)

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
        tare=tare_column,
        unplaced_quantity=unplaced_quantity_column,
    )


def resolve_consolidated_columns(df: pd.DataFrame) -> WarehouseDelayColumnMapping:
    mapping = resolve_columns(df)
    block_column = _find_column(df.columns, BLOCK_COLUMN_ALIASES)
    if not block_column:
        raise WarehouseDelayStructureError(
            "Не найдена обязательная колонка 'Блок' для сводного файла."
        )
    return WarehouseDelayColumnMapping(
        hours=mapping.hours,
        mx_processing=mapping.mx_processing,
        block=block_column,
        tare=mapping.tare,
        unplaced_quantity=mapping.unplaced_quantity,
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


def format_delay_hours(hours: float) -> str:
    total_minutes = max(int(round(hours * 60)), 0)
    whole_hours, minutes = divmod(total_minutes, 60)
    return f"{whole_hours:02d}:{minutes:02d}"


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


def _normalize_text_cell(value: Any) -> str:
    if value is None or pd.isna(value):
        return ""
    return str(value).strip()


def _normalize_quantity(value: Any) -> int | float | str:
    if value is None or pd.isna(value):
        return 0
    if isinstance(value, bool):
        return 0
    if isinstance(value, int):
        return value
    if isinstance(value, float):
        return int(value) if value.is_integer() else value

    text = str(value).strip()
    if not text:
        return 0

    compact = text.replace(" ", "").replace(",", ".")
    if re.fullmatch(r"[+-]?\d+", compact):
        return int(compact)
    if re.fullmatch(r"[+-]?\d+\.\d+", compact):
        numeric_value = float(compact)
        return int(numeric_value) if numeric_value.is_integer() else numeric_value
    return text


def _build_top_without_assignment(
    candidates: Iterable[WarehouseDelayTopItem],
) -> list[WarehouseDelayTopItem]:
    best_by_tare: dict[str, WarehouseDelayTopItem] = {}

    for candidate in candidates:
        tare = _normalize_text_cell(candidate.tare)
        if not tare:
            logger.info(
                "Warehouse delay top skipped row without tare mx_processing=%s delay=%s",
                candidate.mx_processing,
                candidate.delay_display,
            )
            continue

        current = best_by_tare.get(tare)
        if current is None or candidate.delay_hours > current.delay_hours:
            best_by_tare[tare] = WarehouseDelayTopItem(
                tare=tare,
                delay_hours=candidate.delay_hours,
                delay_display=candidate.delay_display,
                mx_processing=_normalize_text_cell(candidate.mx_processing),
                unplaced_quantity=candidate.unplaced_quantity,
            )

    return sorted(
        best_by_tare.values(),
        key=lambda item: item.delay_hours,
        reverse=True,
    )[:10]


def _register_delay_row(
    accumulator: _WarehouseDelayAccumulator,
    *,
    canonical_row_name: str,
    hours: float,
    mx_processing_value: Any,
    tare_value: Any = None,
    unplaced_quantity_value: Any = None,
) -> None:
    all_stats = accumulator.all_rows[canonical_row_name]
    no_assignment_stats = accumulator.no_assignment_rows[canonical_row_name]
    bucket = bucketize_hours(hours)

    accumulator.processed_rows += 1
    all_stats[TOTAL_COLUMN] += 1
    if bucket:
        all_stats[bucket] += 1

    if is_without_assignment(mx_processing_value):
        accumulator.no_assignment_rows_count += 1
        no_assignment_stats[TOTAL_COLUMN] += 1
        if bucket:
            no_assignment_stats[bucket] += 1
        accumulator.top_candidates.append(
            WarehouseDelayTopItem(
                tare=_normalize_text_cell(tare_value),
                delay_hours=hours,
                delay_display=format_delay_hours(hours),
                mx_processing=_normalize_text_cell(mx_processing_value),
                unplaced_quantity=_normalize_quantity(unplaced_quantity_value),
            )
        )


def _calculate_file_statistics(
    df: pd.DataFrame,
    filename: str,
) -> tuple[str, dict[str, int], dict[str, int], WarehouseDelayFileStats, list[WarehouseDelayTopItem]]:
    column_mapping = resolve_columns(df)
    canonical_row_name, matched_by = resolve_canonical_row_name(
        filename, df, column_mapping
    )
    accumulator = _WarehouseDelayAccumulator()

    for _, row in df.iterrows():
        hours = parse_delay_hours(row[column_mapping.hours])
        if hours is None:
            accumulator.invalid_hours_rows += 1
            continue

        _register_delay_row(
            accumulator,
            canonical_row_name=canonical_row_name,
            hours=hours,
            mx_processing_value=row[column_mapping.mx_processing],
            tare_value=row[column_mapping.tare] if column_mapping.tare else None,
            unplaced_quantity_value=(
                row[column_mapping.unplaced_quantity]
                if column_mapping.unplaced_quantity
                else None
            ),
        )

    if accumulator.invalid_hours_rows:
        logger.info(
            "Warehouse delay: skipped rows with unknown hours file=%s count=%s",
            filename,
            accumulator.invalid_hours_rows,
        )

    file_stats = WarehouseDelayFileStats(
        filename=filename,
        canonical_row_name=canonical_row_name,
        processed_rows=accumulator.processed_rows,
        no_assignment_rows=accumulator.no_assignment_rows_count,
        invalid_hours_rows=accumulator.invalid_hours_rows,
        matched_by=matched_by,
    )
    return (
        canonical_row_name,
        dict(accumulator.all_rows[canonical_row_name]),
        dict(accumulator.no_assignment_rows[canonical_row_name]),
        file_stats,
        list(accumulator.top_candidates),
    )


def calculate_file_statistics(
    df: pd.DataFrame,
    filename: str,
) -> tuple[str, dict[str, int], dict[str, int], WarehouseDelayFileStats]:
    canonical_row_name, all_stats, no_assignment_stats, file_stats, _ = (
        _calculate_file_statistics(df, filename)
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
    top_candidates: list[WarehouseDelayTopItem] = []

    for filename, file_path in files:
        try:
            (
                canonical_row_name,
                file_all_stats,
                file_no_assignment_stats,
                file_stats,
                file_top_candidates,
            ) = _calculate_file_statistics(read_warehouse_delay_file(file_path), filename)
        except WarehouseDelayError as exc:
            logger.info(
                "Warehouse delay skipped file filename=%s reason=%s",
                filename,
                exc,
            )
            skipped_files.append(filename)
            continue

        for column, value in file_all_stats.items():
            all_rows[canonical_row_name][column] += value
        for column, value in file_no_assignment_stats.items():
            no_assignment_rows[canonical_row_name][column] += value
        processed_files.append(file_stats)
        top_candidates.extend(file_top_candidates)

    return WarehouseDelayAggregationResult(
        all_rows=all_rows,
        no_assignment_rows=no_assignment_rows,
        processed_files=processed_files,
        skipped_files=skipped_files,
        top_without_assignment=_build_top_without_assignment(top_candidates),
    )


def process_warehouse_delay_consolidated_file(
    file_path: str | Path,
    filename: str | None = None,
) -> WarehouseDelayAggregationResult:
    path = Path(file_path)
    dataframe = read_warehouse_delay_file(path)
    source_name = filename or path.name
    column_mapping = resolve_consolidated_columns(dataframe)
    accumulator = _WarehouseDelayAccumulator()

    for row_index, row in dataframe.iterrows():
        block_value = row[column_mapping.block]
        canonical_row_name = map_filename_to_canonical_row(str(block_value))
        if not canonical_row_name:
            accumulator.skipped_unknown_rows += 1
            logger.info(
                "Warehouse delay consolidated skipped row with unknown block file=%s row=%s block=%r",
                source_name,
                row_index + 2,
                block_value,
            )
            continue

        hours = parse_delay_hours(row[column_mapping.hours])
        if hours is None:
            accumulator.invalid_hours_rows += 1
            logger.info(
                "Warehouse delay consolidated skipped row with invalid hours file=%s row=%s value=%r",
                source_name,
                row_index + 2,
                row[column_mapping.hours],
            )
            continue

        _register_delay_row(
            accumulator,
            canonical_row_name=canonical_row_name,
            hours=hours,
            mx_processing_value=row[column_mapping.mx_processing],
            tare_value=row[column_mapping.tare] if column_mapping.tare else None,
            unplaced_quantity_value=(
                row[column_mapping.unplaced_quantity]
                if column_mapping.unplaced_quantity
                else None
            ),
        )

    if accumulator.processed_rows == 0:
        raise WarehouseDelayUnrecognizedFile(
            "Не удалось распознать сводный файл: проверьте колонку 'Блок' и формат времени простоя."
        )

    file_stats = WarehouseDelayFileStats(
        filename=source_name,
        canonical_row_name=None,
        processed_rows=accumulator.processed_rows,
        no_assignment_rows=accumulator.no_assignment_rows_count,
        invalid_hours_rows=accumulator.invalid_hours_rows,
        matched_by="block",
        skipped_unknown_rows=accumulator.skipped_unknown_rows,
    )
    return WarehouseDelayAggregationResult(
        all_rows=accumulator.all_rows,
        no_assignment_rows=accumulator.no_assignment_rows,
        processed_files=[file_stats],
        skipped_files=[],
        top_without_assignment=_build_top_without_assignment(accumulator.top_candidates),
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
            TOTAL_COLUMN,
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


def build_warehouse_delay_top_rows(
    aggregation: WarehouseDelayAggregationResult,
) -> list[list[str | int | float]]:
    rows: list[list[str | int | float]] = [
        [TOP_WITHOUT_ASSIGNMENT_TITLE],
        TOP_WITHOUT_ASSIGNMENT_HEADERS,
    ]
    for item in aggregation.top_without_assignment:
        rows.append(
            [
                item.tare,
                item.delay_display,
                item.mx_processing,
                item.unplaced_quantity,
            ]
        )
    return rows


def build_warehouse_delay_sheet_matrix(
    aggregation: WarehouseDelayAggregationResult,
    report_date: date,
) -> list[list[str | int | float]]:
    left_rows = build_warehouse_delay_sheet_rows(aggregation, report_date)
    right_rows = build_warehouse_delay_top_rows(aggregation)
    if not right_rows:
        return left_rows

    left_width = max((len(row) for row in left_rows), default=0)
    right_width = max((len(row) for row in right_rows), default=0)
    spacer = ["", ""]
    total_height = max(len(left_rows), len(right_rows))
    matrix: list[list[str | int | float]] = []

    for index in range(total_height):
        left_row = list(left_rows[index]) if index < len(left_rows) else []
        right_row = list(right_rows[index]) if index < len(right_rows) else []
        padded_left_row = left_row + [""] * (left_width - len(left_row))
        padded_right_row = right_row + [""] * (right_width - len(right_row))
        matrix.append([*padded_left_row, *spacer, *padded_right_row])

    return matrix
