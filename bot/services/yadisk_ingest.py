from __future__ import annotations

import logging
import math
import re
import sqlite3
from collections.abc import Iterable
from datetime import date, datetime
from pathlib import Path
from typing import Any

import pandas as pd

from bot.db import (
    calculate_row_hash,
    finish_import,
    get_db_connection,
    get_existing_raw_yadisk_row,
    insert_raw_yadisk_row_if_new,
    normalize_empty_value,
    serialize_json,
    start_import,
    update_raw_yadisk_match,
    utc_now_iso,
    find_case_candidates_by_item_name,
    find_case_candidates_by_shk,
    find_case_candidates_by_tare_transfer,
)

logger = logging.getLogger(__name__)

SOURCE_KIND_NO_MOVE = "no_move"
SOURCE_KIND_24H = "24h"
ENABLE_ITEM_NAME_AUTO_MATCH = False
RAW_INGEST_PROGRESS_LOG_EVERY_ROWS = 200

_SPACE_RE = re.compile(r"\s+")
_EMPTY_TEXT_VALUES = {"", "none", "null", "nan"}

_SOURCE_KEYWORDS: dict[str, dict[str, tuple[str, ...]]] = {
    SOURCE_KIND_NO_MOVE: {
        "shk": ("шк", "идентификатор товара", "sku"),
        "tare_transfer": ("гофра", "тара/передача", "тара", "передача"),
        "item_name": ("наименование", "товар"),
        "amount": ("стоимость", "сумма"),
        "qty_shk": ("количество шк", "кол-во шк", "количество"),
        "last_movement_at": ("дата последнего движения",),
        "writeoff_started_at": ("начало списания",),
        "example_related_shk": ("пример", "сопутств"),
    },
    SOURCE_KIND_24H: {
        "block_id": ("id блока", "блока"),
        "shk": ("идентификатор товара", "шк", "sku"),
        "tare_transfer": ("гофра", "тара/передача", "тара", "id тары"),
        "item_name": ("наименование", "товар"),
        "amount": ("стоимость", "сумма"),
        "qty_shk": ("количество шк", "кол-во шк", "количество"),
        "writeoff_started_at": ("прогноз", "начнет", "спис"),
        "last_movement_at": ("дата последнего движения",),
        "example_related_shk": ("пример", "сопутств"),
    },
}

_REQUIRED_MAPPINGS = {
    SOURCE_KIND_NO_MOVE: ("shk", "tare_transfer", "amount"),
    SOURCE_KIND_24H: ("block_id", "shk", "writeoff_started_at", "amount"),
}


def _normalize_header_name(value: Any) -> str:
    text = str(value or "").replace("\xa0", " ").strip().lower().replace("ё", "е")
    text = re.sub(r"\s*/\s*", "/", text)
    text = _SPACE_RE.sub(" ", text)
    return text.strip(" .:")


def _unique_storage_keys(headers: Iterable[Any]) -> list[dict[str, str]]:
    seen: dict[str, int] = {}
    result: list[dict[str, str]] = []
    for index, header in enumerate(headers):
        original = str(header).strip() if header is not None else ""
        normalized = _normalize_header_name(original)
        storage_key = normalized or f"column_{index + 1}"
        seen[storage_key] = seen.get(storage_key, 0) + 1
        if seen[storage_key] > 1:
            storage_key = f"{storage_key}__{seen[storage_key]}"
        result.append(
            {
                "original": original or f"column_{index + 1}",
                "normalized": normalized,
                "storage_key": storage_key,
            }
        )
    return result


def _normalize_scalar(value: Any) -> Any:
    if value is None:
        return None
    if isinstance(value, float) and math.isnan(value):
        return None
    if pd.isna(value):
        return None
    if isinstance(value, pd.Timestamp):
        return value.to_pydatetime().replace(microsecond=0).isoformat()
    if isinstance(value, datetime):
        return value.replace(microsecond=0).isoformat()
    if isinstance(value, date):
        return datetime.combine(value, datetime.min.time()).isoformat()
    if isinstance(value, str):
        normalized = value.replace("\xa0", " ").strip()
        if normalized.lower() in _EMPTY_TEXT_VALUES:
            return None
        return normalized or None
    if isinstance(value, float) and value.is_integer():
        return int(value)
    return value


def _parse_float(value: Any) -> float | None:
    normalized = _normalize_scalar(value)
    if normalized is None:
        return None
    if isinstance(normalized, (int, float)):
        return float(normalized)
    text = str(normalized).replace(" ", "").replace(",", ".")
    try:
        return float(text)
    except ValueError:
        return None


def _parse_int(value: Any) -> int | None:
    normalized = _normalize_scalar(value)
    if normalized is None:
        return None
    if isinstance(normalized, int):
        return normalized
    if isinstance(normalized, float):
        return int(normalized)
    text = str(normalized).replace(" ", "").replace(",", ".")
    try:
        return int(float(text))
    except ValueError:
        return None


def _parse_datetime(value: Any) -> str | None:
    normalized = _normalize_scalar(value)
    if normalized is None:
        return None
    if isinstance(normalized, str):
        parsed = pd.to_datetime(normalized, dayfirst=True, errors="coerce")
    else:
        parsed = pd.to_datetime(normalized, errors="coerce")
    if pd.isna(parsed):
        return None
    return parsed.to_pydatetime().replace(microsecond=0).isoformat()


def _find_column(headers: list[dict[str, str]], keywords: Iterable[str]) -> str | None:
    for header in headers:
        normalized = header["normalized"]
        for keyword in keywords:
            if keyword in normalized:
                return header["storage_key"]
    return None


def _map_source_columns(
    source_kind: str,
    headers: list[dict[str, str]],
) -> dict[str, str]:
    keywords = _SOURCE_KEYWORDS.get(source_kind)
    if not keywords:
        raise ValueError(f"Unsupported source_kind: {source_kind}")

    mapping: dict[str, str] = {}
    for field_name, field_keywords in keywords.items():
        column_name = _find_column(headers, field_keywords)
        if column_name:
            mapping[field_name] = column_name
    return mapping


def _normalize_raw_values(
    row_values: list[Any],
    headers: list[dict[str, str]],
) -> dict[str, Any]:
    padded = list(row_values[: len(headers)])
    if len(padded) < len(headers):
        padded.extend([None] * (len(headers) - len(padded)))

    raw_values: dict[str, Any] = {}
    for header_meta, raw_value in zip(headers, padded):
        raw_values[header_meta["storage_key"]] = _normalize_scalar(raw_value)
    return raw_values


def _sheet_has_required_columns(source_kind: str, mapping: dict[str, str]) -> bool:
    return all(mapping.get(field_name) for field_name in _REQUIRED_MAPPINGS[source_kind])


def normalize_yadisk_row(
    *,
    source_kind: str,
    source_sheet_name: str,
    source_row_number: int,
    raw_values: dict[str, Any],
    mapped_columns: dict[str, str],
) -> dict[str, Any]:
    shk = normalize_empty_value(raw_values.get(mapped_columns.get("shk", "")))
    tare_transfer = normalize_empty_value(
        raw_values.get(mapped_columns.get("tare_transfer", ""))
    )
    item_name = normalize_empty_value(raw_values.get(mapped_columns.get("item_name", "")))
    amount = _parse_float(raw_values.get(mapped_columns.get("amount", "")))
    qty_shk = _parse_int(raw_values.get(mapped_columns.get("qty_shk", "")))
    last_movement_at = _parse_datetime(
        raw_values.get(mapped_columns.get("last_movement_at", ""))
    )
    writeoff_started_at = _parse_datetime(
        raw_values.get(mapped_columns.get("writeoff_started_at", ""))
    )
    example_related_shk = normalize_empty_value(
        raw_values.get(mapped_columns.get("example_related_shk", ""))
    )

    extracted_fields = {
        "shk": shk,
        "tare_transfer": tare_transfer,
        "item_name": item_name,
        "amount": amount,
        "qty_shk": qty_shk,
        "last_movement_at": last_movement_at,
        "writeoff_started_at": writeoff_started_at,
        "example_related_shk": example_related_shk,
    }
    if mapped_columns.get("block_id"):
        extracted_fields["block_id"] = normalize_empty_value(
            raw_values.get(mapped_columns["block_id"])
        )

    normalized_json = {
        "source_kind": source_kind,
        "source_sheet_name": source_sheet_name,
        "source_row_number": source_row_number,
        "raw_values": raw_values,
        "extracted_fields": extracted_fields,
    }
    row_hash = compute_yadisk_row_hash(
        {
            "source_kind": source_kind,
            "raw_values": raw_values,
        }
    )

    return {
        "source_kind": source_kind,
        "source_sheet_name": source_sheet_name,
        "source_row_number": int(source_row_number),
        "raw_values": raw_values,
        "normalized_json": normalized_json,
        "row_hash": row_hash,
        "shk": shk,
        "tare_transfer": tare_transfer,
        "item_name": item_name,
        "amount": amount,
        "qty_shk": qty_shk,
        "last_movement_at": last_movement_at,
        "writeoff_started_at": writeoff_started_at,
        "example_related_shk": example_related_shk,
        "is_empty": not any(value is not None for value in raw_values.values()),
    }


def compute_yadisk_row_hash(value: Any) -> str:
    return calculate_row_hash(value)


def _read_normalized_workbook_rows(
    file_path: str | Path,
    source_kind: str,
) -> list[dict[str, Any]]:
    normalized_rows: list[dict[str, Any]] = []

    with pd.ExcelFile(file_path, engine="openpyxl") as workbook:
        for sheet_name in workbook.sheet_names:
            data_frame = workbook.parse(sheet_name=sheet_name, dtype=object)
            headers = _unique_storage_keys(data_frame.columns.tolist())
            mapping = _map_source_columns(source_kind, headers)
            if not _sheet_has_required_columns(source_kind, mapping):
                continue

            for row_index, row_values in enumerate(
                data_frame.itertuples(index=False, name=None),
                start=2,
            ):
                raw_values = _normalize_raw_values(list(row_values), headers)
                normalized_row = normalize_yadisk_row(
                    source_kind=source_kind,
                    source_sheet_name=sheet_name,
                    source_row_number=row_index,
                    raw_values=raw_values,
                    mapped_columns=mapping,
                )
                if normalized_row["is_empty"]:
                    continue
                normalized_rows.append(normalized_row)

    if not normalized_rows:
        raise ValueError(f"No supported sheets found for source_kind={source_kind}")
    return normalized_rows


def match_raw_row_to_case(
    *,
    shk: str | None = None,
    tare_transfer: str | None = None,
    item_name: str | None = None,
    enable_item_name_auto_match: bool = ENABLE_ITEM_NAME_AUTO_MATCH,
    connection: sqlite3.Connection | None = None,
    db_path: str | Path | None = None,
) -> dict[str, Any]:
    normalized_shk = normalize_empty_value(shk)
    if normalized_shk:
        candidates = find_case_candidates_by_shk(
            normalized_shk,
            limit=3,
            connection=connection,
            db_path=db_path,
        )
        if len(candidates) == 1:
            return {
                "matched_case_id": candidates[0]["case_id"],
                "match_method": "shk",
                "match_confidence": "high",
                "link_decision_reason": "unique exact shk match",
            }
        if len(candidates) > 1:
            return {
                "matched_case_id": None,
                "match_method": "shk",
                "match_confidence": "ambiguous",
                "link_decision_reason": "multiple cases found by shk",
            }

    normalized_tare_transfer = normalize_empty_value(tare_transfer)
    if normalized_tare_transfer:
        candidates = find_case_candidates_by_tare_transfer(
            normalized_tare_transfer,
            limit=3,
            connection=connection,
            db_path=db_path,
        )
        if len(candidates) == 1:
            return {
                "matched_case_id": candidates[0]["case_id"],
                "match_method": "tare_transfer",
                "match_confidence": "medium",
                "link_decision_reason": "unique exact tare_transfer match",
            }
        if len(candidates) > 1:
            return {
                "matched_case_id": None,
                "match_method": "tare_transfer",
                "match_confidence": "ambiguous",
                "link_decision_reason": "multiple cases found by tare_transfer",
            }

    normalized_item_name = normalize_empty_value(item_name)
    if normalized_item_name:
        if not enable_item_name_auto_match:
            return {
                "matched_case_id": None,
                "match_method": "none",
                "match_confidence": "none",
                "link_decision_reason": "item_name auto-match skipped in ingest fast path",
                "item_name_auto_match_skipped": True,
            }
        candidates = find_case_candidates_by_item_name(
            normalized_item_name,
            limit=3,
            connection=connection,
            db_path=db_path,
        )
        if len(candidates) == 1:
            return {
                "matched_case_id": candidates[0]["case_id"],
                "match_method": "item_name",
                "match_confidence": "low",
                "link_decision_reason": "unique exact item_name match",
            }
        if len(candidates) > 1:
            return {
                "matched_case_id": None,
                "match_method": "item_name",
                "match_confidence": "ambiguous",
                "link_decision_reason": "multiple cases found by item_name",
            }

    return {
        "matched_case_id": None,
        "match_method": "none",
        "match_confidence": "none",
        "link_decision_reason": "no exact case candidate found",
    }


def link_raw_row_to_case(
    *,
    raw_row_id: int,
    match_result: dict[str, Any],
    connection: sqlite3.Connection | None = None,
    db_path: str | Path | None = None,
) -> bool:
    matched_case_id = normalize_empty_value(match_result.get("matched_case_id"))
    linked_at = utc_now_iso() if matched_case_id else None
    return update_raw_yadisk_match(
        raw_row_id=raw_row_id,
        matched_case_id=matched_case_id,
        match_method=match_result.get("match_method"),
        match_confidence=match_result.get("match_confidence"),
        linked_at=linked_at,
        link_decision_reason=match_result.get("link_decision_reason"),
        connection=connection,
        db_path=db_path,
    )


def _resolve_import_source_type(file_info: dict[str, Any]) -> str:
    source = str(file_info.get("source") or "").lower()
    if "yadisk" in source:
        return "yadisk"
    return "excel"


def _resolve_source_identity(file_info: dict[str, Any]) -> str | None:
    source_path = normalize_empty_value(file_info.get("source_path"))
    if source_path:
        return source_path
    source_url = normalize_empty_value(file_info.get("source_url"))
    if source_url:
        return source_url
    return normalize_empty_value(file_info.get("filename"))


def ingest_yadisk_rows(
    *,
    file_path: str | Path,
    source_kind: str,
    file_info: dict[str, Any],
    db_path: str | Path,
) -> dict[str, Any]:
    conn = get_db_connection(db_path)
    import_id: int | None = None
    rows_read = 0
    rows_written = 0
    rows_linked = 0
    rows_deduped = 0
    rows_matched_by_shk = 0
    rows_matched_by_tare_transfer = 0
    rows_skipped_item_name_auto_match = 0
    item_name_skip_logged = False
    source_identity = _resolve_source_identity(file_info)
    source_file_name = normalize_empty_value(file_info.get("filename"))

    try:
        import_id = start_import(
            source_type=_resolve_import_source_type(file_info),
            source_name=source_file_name,
            source_path=source_identity,
            connection=conn,
        )
        conn.commit()

        normalized_rows = _read_normalized_workbook_rows(file_path, source_kind)
        imported_at = utc_now_iso()

        for normalized_row in normalized_rows:
            rows_read += 1
            inserted_row_id = insert_raw_yadisk_row_if_new(
                row_hash=normalized_row["row_hash"],
                import_batch_id=import_id,
                source_file_name=source_file_name,
                source_path=source_identity,
                source_kind=source_kind,
                source_sheet_name=normalized_row["source_sheet_name"],
                source_row_number=normalized_row["source_row_number"],
                shk=normalized_row["shk"],
                tare_transfer=normalized_row["tare_transfer"],
                item_name=normalized_row["item_name"],
                amount=normalized_row["amount"],
                qty_shk=normalized_row["qty_shk"],
                last_movement_at=normalized_row["last_movement_at"],
                writeoff_started_at=normalized_row["writeoff_started_at"],
                example_related_shk=normalized_row["example_related_shk"],
                normalized_json=normalized_row["normalized_json"],
                imported_at=imported_at,
                connection=conn,
            )

            existing_row = None
            raw_row_id = inserted_row_id
            if raw_row_id is None:
                existing_row = get_existing_raw_yadisk_row(
                    row_hash=normalized_row["row_hash"],
                    source_path=source_identity,
                    source_file_name=source_file_name,
                    source_kind=source_kind,
                    source_sheet_name=normalized_row["source_sheet_name"],
                    connection=conn,
                )
                if existing_row is None:
                    logger.warning(
                        "raw_yadisk_rows duplicate check failed to return existing row: source=%s kind=%s hash=%s",
                        source_identity,
                        source_kind,
                        normalized_row["row_hash"],
                    )
                    continue
                raw_row_id = int(existing_row["id"])
                rows_deduped += 1
            else:
                rows_written += 1

            match_result = match_raw_row_to_case(
                shk=normalized_row["shk"],
                tare_transfer=normalized_row["tare_transfer"],
                item_name=normalized_row["item_name"],
                enable_item_name_auto_match=ENABLE_ITEM_NAME_AUTO_MATCH,
                connection=conn,
            )
            if match_result.get("matched_case_id"):
                if match_result.get("match_method") == "shk":
                    rows_matched_by_shk += 1
                elif match_result.get("match_method") == "tare_transfer":
                    rows_matched_by_tare_transfer += 1
            elif match_result.get("item_name_auto_match_skipped"):
                rows_skipped_item_name_auto_match += 1
                if not item_name_skip_logged:
                    logger.info(
                        "Raw ingest fast path: item_name auto-match is disabled; unresolved rows will stay pending for manual review. kind=%s source=%s import_id=%s",
                        source_kind,
                        source_identity,
                        import_id,
                    )
                    item_name_skip_logged = True
            should_link = inserted_row_id is not None
            if existing_row is not None:
                should_link = any(
                    existing_row.get(field_name) is None
                    for field_name in (
                        "matched_case_id",
                        "match_method",
                        "match_confidence",
                        "link_decision_reason",
                    )
                )
                if (
                    normalize_empty_value(existing_row.get("matched_case_id"))
                    == normalize_empty_value(match_result.get("matched_case_id"))
                    and normalize_empty_value(existing_row.get("match_method"))
                    == normalize_empty_value(match_result.get("match_method"))
                    and normalize_empty_value(existing_row.get("match_confidence"))
                    == normalize_empty_value(match_result.get("match_confidence"))
                ):
                    should_link = False

            if should_link and raw_row_id is not None:
                updated = link_raw_row_to_case(
                    raw_row_id=int(raw_row_id),
                    match_result=match_result,
                    connection=conn,
                )
                if updated:
                    rows_linked += 1

            if rows_read % RAW_INGEST_PROGRESS_LOG_EVERY_ROWS == 0:
                logger.info(
                    "Raw ingest progress: kind=%s source=%s import_id=%s rows_processed=%s rows_inserted=%s rows_matched_by_shk=%s rows_matched_by_tare_transfer=%s rows_skipped_auto_match=%s rows_deduped=%s",
                    source_kind,
                    source_identity,
                    import_id,
                    rows_read,
                    rows_written,
                    rows_matched_by_shk,
                    rows_matched_by_tare_transfer,
                    rows_skipped_item_name_auto_match,
                    rows_deduped,
                )

        logger.info(
            "Raw ingest summary: kind=%s source=%s import_id=%s rows_processed=%s rows_inserted=%s rows_matched_by_shk=%s rows_matched_by_tare_transfer=%s rows_skipped_auto_match=%s rows_deduped=%s rows_linked=%s",
            source_kind,
            source_identity,
            import_id,
            rows_read,
            rows_written,
            rows_matched_by_shk,
            rows_matched_by_tare_transfer,
            rows_skipped_item_name_auto_match,
            rows_deduped,
            rows_linked,
        )

        finish_import(
            import_id=import_id,
            status="success",
            rows_read=rows_read,
            rows_written=rows_written,
            connection=conn,
        )
        conn.commit()

        return {
            "import_id": import_id,
            "source_kind": source_kind,
            "rows_read": rows_read,
            "rows_written": rows_written,
            "rows_linked": rows_linked,
            "rows_deduped": rows_deduped,
            "rows_matched_by_shk": rows_matched_by_shk,
            "rows_matched_by_tare_transfer": rows_matched_by_tare_transfer,
            "rows_skipped_item_name_auto_match": rows_skipped_item_name_auto_match,
            "source_name": source_file_name,
            "source_path": source_identity,
        }
    except Exception as exc:
        conn.rollback()
        if import_id is not None:
            finish_import(
                import_id=import_id,
                status="failed",
                rows_read=rows_read,
                rows_written=rows_written,
                error_text=str(exc),
                connection=conn,
            )
            conn.commit()
        raise
    finally:
        conn.close()


def summarize_normalized_row(normalized_row: dict[str, Any]) -> str:
    return serialize_json(
        {
            "source_kind": normalized_row.get("source_kind"),
            "source_sheet_name": normalized_row.get("source_sheet_name"),
            "source_row_number": normalized_row.get("source_row_number"),
            "row_hash": normalized_row.get("row_hash"),
        }
    )
