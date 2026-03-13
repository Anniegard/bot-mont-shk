from __future__ import annotations

import logging
import re
from typing import Any
from uuid import uuid4

import gspread
from gspread.utils import rowcol_to_a1

from bot.constants import CASE_ID_COLUMN_NAME, CASES_MASTER_SHEET_NAME
from bot.db import (
    calculate_row_hash,
    finish_import,
    get_case_by_case_id,
    get_db_connection,
    insert_case_version_if_changed,
    insert_import,
    insert_raw_sheet_row_if_new,
    normalize_empty_value,
    upsert_case,
    upsert_case_item,
    upsert_sheet_sync_state,
    utc_now_iso,
)
from bot.services.sheets import get_case_id_column_index, get_worksheet

logger = logging.getLogger(__name__)

_SPACE_RE = re.compile(r"\s+")

REQUIRED_FIELD_LABELS = {
    "review_date": "Дата разбора",
    "analyst": "Аналитик",
    "item_name": "Наименование",
    "culprit_id": "ID виновного",
    "comment_text": "Комментарий",
    "example_related_shk": "Пример попутного/обработанного шк",
    "action_taken": "Что предпринято",
    "movement_status": "Движение товара",
}

OPTIONAL_FIELD_LABELS = {
    "report_request": "Отчет/Запрос",
    "warehouse": "Склад",
    "tare_transfer": "Тара/передача",
    "shk": "ШК",
    "amount": "Сумма",
    "qty_shk": "Количество ШК",
    "last_movement_at": "Дата последнего движения",
    "writeoff_started_at": "Начало списания товара",
}

HEADER_ALIASES = {
    "дата разбора": "review_date",
    "аналитик": "analyst",
    "наименование": "item_name",
    "id виновного": "culprit_id",
    "коментарий": "comment_text",
    "комментарий": "comment_text",
    "пример попутного/обработанного шк": "example_related_shk",
    "пример попутного / обработанного шк": "example_related_shk",
    "что предпринято": "action_taken",
    "движение товара": "movement_status",
    "отчет/запрос": "report_request",
    "отчет / запрос": "report_request",
    "склад": "warehouse",
    "тара/передача": "tare_transfer",
    "тара / передача": "tare_transfer",
    "шк": "shk",
    "сумма": "amount",
    "количество шк": "qty_shk",
    "кол-во шк": "qty_shk",
    "дата последнего движ-я по истории": "last_movement_at",
    "дата последнего движения по истории": "last_movement_at",
    "дата последнего движения": "last_movement_at",
    "дата последнего движ-я": "last_movement_at",
    "начало списания товара": "writeoff_started_at",
    "начало списания": "writeoff_started_at",
}

TEXT_CASE_FIELDS = {
    "review_date",
    "analyst",
    "item_name",
    "culprit_id",
    "comment_text",
    "example_related_shk",
    "action_taken",
    "movement_status",
    "report_request",
    "warehouse",
    "tare_transfer",
    "shk",
    "last_movement_at",
    "writeoff_started_at",
}


def _normalize_header_name(value: Any) -> str:
    text = str(value or "").replace("\xa0", " ").strip().lower().replace("ё", "е")
    text = re.sub(r"\s*/\s*", "/", text)
    text = _SPACE_RE.sub(" ", text)
    return text.strip(" .:")


def _normalize_cell_value(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).replace("\xa0", " ").strip()
    return text or None


def _parse_float(value: Any) -> float | None:
    normalized = normalize_empty_value(value)
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
    normalized = normalize_empty_value(value)
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


def normalize_sheet_headers(headers: list[Any]) -> list[dict[str, Any]]:
    normalized_headers: list[dict[str, Any]] = []
    seen_keys: dict[str, int] = {}

    for index, header in enumerate(headers):
        original = str(header).strip() if header is not None else ""
        normalized_name = _normalize_header_name(original)
        is_case_id = normalized_name == CASE_ID_COLUMN_NAME
        field_name = None if is_case_id else HEADER_ALIASES.get(normalized_name)
        storage_key = (
            CASE_ID_COLUMN_NAME if is_case_id else field_name or normalized_name
        )
        if not storage_key:
            storage_key = f"column_{index + 1}"

        seen_keys[storage_key] = seen_keys.get(storage_key, 0) + 1
        unique_storage_key = storage_key
        if seen_keys[storage_key] > 1:
            unique_storage_key = f"{storage_key}__{seen_keys[storage_key]}"

        normalized_headers.append(
            {
                "index": index,
                "header": original or f"column_{index + 1}",
                "normalized_name": normalized_name,
                "field_name": field_name,
                "storage_key": unique_storage_key,
                "is_case_id": is_case_id,
            }
        )

    return normalized_headers


def ensure_case_id_column(worksheet: gspread.Worksheet) -> tuple[list[str], int]:
    headers = worksheet.row_values(1)
    case_id_index = get_case_id_column_index(headers)
    if case_id_index is not None:
        return headers, case_id_index

    target_column = max(len(headers) + 1, 1)
    if target_column > worksheet.col_count:
        worksheet.add_cols(target_column - worksheet.col_count)
    worksheet.update_cell(1, target_column, CASE_ID_COLUMN_NAME)

    padded_headers = list(headers)
    if len(padded_headers) < target_column - 1:
        padded_headers.extend([""] * ((target_column - 1) - len(padded_headers)))
    padded_headers.append(CASE_ID_COLUMN_NAME)
    return padded_headers, target_column - 1


def _assert_required_headers(headers: list[dict[str, Any]]) -> None:
    present_fields = {
        header["field_name"] for header in headers if header["field_name"]
    }
    missing_labels = [
        label
        for field_name, label in REQUIRED_FIELD_LABELS.items()
        if field_name not in present_fields
    ]
    if missing_labels:
        raise ValueError(
            "Master sheet is missing required columns: " + ", ".join(missing_labels)
        )


def normalize_case_row(
    headers: list[dict[str, Any]],
    row: list[Any],
    row_number: int,
) -> dict[str, Any]:
    padded_row = list(row[: len(headers)])
    if len(padded_row) < len(headers):
        padded_row.extend([""] * (len(headers) - len(padded_row)))

    raw_row: dict[str, Any] = {}
    snapshot: dict[str, Any] = {
        field_name: None for field_name in REQUIRED_FIELD_LABELS
    }
    case_fields: dict[str, Any] = {}
    case_id: str | None = None

    for header_meta, raw_value in zip(headers, padded_row):
        normalized_value = _normalize_cell_value(raw_value)
        raw_key = header_meta["header"]
        if raw_key in raw_row:
            raw_key = f"{raw_key}__{header_meta['index'] + 1}"
        raw_row[raw_key] = normalized_value

        if header_meta["is_case_id"]:
            case_id = normalized_value
            continue

        field_name = header_meta["field_name"]
        if field_name in TEXT_CASE_FIELDS:
            if field_name not in case_fields or case_fields[field_name] is None:
                case_fields[field_name] = normalized_value
                snapshot[field_name] = normalized_value
            continue

        if field_name == "amount":
            parsed_value = _parse_float(normalized_value)
            case_fields[field_name] = parsed_value
            snapshot[field_name] = (
                parsed_value if parsed_value is not None else normalized_value
            )
            continue

        if field_name == "qty_shk":
            parsed_value = _parse_int(normalized_value)
            case_fields[field_name] = parsed_value
            snapshot[field_name] = (
                parsed_value if parsed_value is not None else normalized_value
            )
            continue

        storage_key = header_meta["storage_key"]
        snapshot[storage_key] = normalized_value

    business_values = {
        key: value
        for key, value in snapshot.items()
        if normalize_empty_value(value) is not None
    }
    is_empty = not business_values
    row_hash = compute_case_row_hash(snapshot)
    missing_required_fields = [
        field_name
        for field_name in REQUIRED_FIELD_LABELS
        if normalize_empty_value(snapshot.get(field_name)) is None
    ]

    return {
        "sheet_row_number": row_number,
        "case_id": case_id,
        "raw_row": raw_row,
        "snapshot": snapshot,
        "case_fields": case_fields,
        "row_hash": row_hash,
        "is_empty": is_empty,
        "is_valid": not missing_required_fields,
        "missing_required_fields": missing_required_fields,
    }


def compute_case_row_hash(snapshot: dict[str, Any]) -> str:
    return calculate_row_hash(snapshot)


def compute_sheet_hash(rows: list[dict[str, Any]]) -> str:
    payload = [
        {
            "sheet_row_number": row["sheet_row_number"],
            "row_hash": row["row_hash"],
        }
        for row in rows
    ]
    return calculate_row_hash(payload)


def read_master_sheet_rows(
    client: gspread.Client,
    spreadsheet_id: str,
    sheet_name: str = CASES_MASTER_SHEET_NAME,
) -> dict[str, Any]:
    worksheet = get_worksheet(client, spreadsheet_id, sheet_name)
    existing_headers = worksheet.row_values(1)
    preview_headers = list(existing_headers)
    if get_case_id_column_index(preview_headers) is None:
        preview_headers.append(CASE_ID_COLUMN_NAME)
    _assert_required_headers(normalize_sheet_headers(preview_headers))

    headers, case_id_index = ensure_case_id_column(worksheet)
    values = worksheet.get_all_values()
    if not values:
        values = [headers]
    elif len(values[0]) < len(headers):
        values[0] = headers

    normalized_headers = normalize_sheet_headers(values[0])

    updates: list[dict[str, Any]] = []
    rows: list[dict[str, Any]] = []

    for row_number, row in enumerate(values[1:], start=2):
        normalized_row = normalize_case_row(normalized_headers, row, row_number)
        if normalized_row["is_empty"]:
            continue

        if not normalized_row["case_id"]:
            generated_case_id = uuid4().hex
            padded_row = list(row[: len(normalized_headers)])
            if len(padded_row) < len(normalized_headers):
                padded_row.extend([""] * (len(normalized_headers) - len(padded_row)))
            padded_row[case_id_index] = generated_case_id
            updates.append(
                {
                    "range": rowcol_to_a1(row_number, case_id_index + 1),
                    "values": [[generated_case_id]],
                }
            )
            normalized_row = normalize_case_row(
                normalized_headers, padded_row, row_number
            )

        rows.append(normalized_row)

    if updates:
        worksheet.batch_update(updates)

    return {
        "worksheet": worksheet,
        "sheet_name": worksheet.title,
        "headers": normalized_headers,
        "rows": rows,
        "case_id_updates": len(updates),
    }


def _build_case_version_payload(row: dict[str, Any]) -> dict[str, Any]:
    return {
        "snapshot": row["snapshot"],
        "raw_row": row["raw_row"],
    }


def _build_raw_sheet_payload(row: dict[str, Any]) -> dict[str, Any]:
    return {
        "case_id": row["case_id"],
        "snapshot": row["snapshot"],
        "raw_row": row["raw_row"],
        "missing_required_fields": row["missing_required_fields"],
    }


def _build_case_fields(row: dict[str, Any]) -> dict[str, Any]:
    case_fields = dict(row["case_fields"])
    case_fields["source_row_hash"] = row["row_hash"]
    return case_fields


def _case_requires_update(
    existing_case: dict[str, Any] | None,
    sheet_name: str,
    row_number: int,
    case_fields: dict[str, Any],
) -> bool:
    if existing_case is None:
        return True
    if existing_case.get("source_sheet_name") != sheet_name:
        return True
    if existing_case.get("sheet_row_number") != row_number:
        return True
    if int(existing_case.get("is_active") or 0) != 1:
        return True
    for field_name, field_value in case_fields.items():
        if existing_case.get(field_name) != field_value:
            return True
    return False


def upsert_case_from_sheet_row(
    sheet_name: str,
    row: dict[str, Any],
    imported_at: str,
    connection,
) -> dict[str, int]:
    existing_case = get_case_by_case_id(row["case_id"], connection=connection)
    case_fields = _build_case_fields(row)
    if existing_case is None or existing_case.get("source_row_hash") != row["row_hash"]:
        case_fields["last_synced_at"] = imported_at

    case_changed = _case_requires_update(
        existing_case,
        sheet_name=sheet_name,
        row_number=row["sheet_row_number"],
        case_fields=case_fields,
    )
    upsert_case(
        case_id=row["case_id"],
        source_sheet_name=sheet_name,
        sheet_row_number=row["sheet_row_number"],
        is_active=True,
        updated_at=imported_at,
        case_fields=case_fields,
        connection=connection,
    )

    version_id = insert_case_version_if_changed(
        case_id=row["case_id"],
        row_hash=row["row_hash"],
        raw_snapshot_json=_build_case_version_payload(row),
        sheet_row_number=row["sheet_row_number"],
        imported_at=imported_at,
        connection=connection,
    )
    item_id = upsert_case_item(
        case_id=row["case_id"],
        shk=row["case_fields"].get("shk"),
        tare_transfer=row["case_fields"].get("tare_transfer"),
        item_name=row["case_fields"].get("item_name"),
        amount=row["case_fields"].get("amount"),
        qty_shk=row["case_fields"].get("qty_shk"),
        last_movement_at=row["case_fields"].get("last_movement_at"),
        writeoff_started_at=row["case_fields"].get("writeoff_started_at"),
        example_related_shk=row["case_fields"].get("example_related_shk"),
        created_at=imported_at,
        connection=connection,
    )

    return {
        "case_changed": 1 if case_changed else 0,
        "version_inserted": 1 if version_id is not None else 0,
        "item_written": 1 if item_id is not None else 0,
    }


def sync_cases_from_master_sheet(
    client: gspread.Client,
    spreadsheet_id: str,
    db_path,
    sheet_name: str = CASES_MASTER_SHEET_NAME,
) -> dict[str, Any]:
    conn = get_db_connection(db_path)
    import_id: int | None = None
    rows_read = 0
    rows_written = 0

    try:
        import_id = insert_import(
            source_type="google_sheets",
            source_name=spreadsheet_id,
            sheet_name=sheet_name,
            status="running",
            connection=conn,
        )
        conn.commit()

        sheet_data = read_master_sheet_rows(client, spreadsheet_id, sheet_name)
        imported_at = utc_now_iso()
        invalid_rows = 0

        for row in sheet_data["rows"]:
            rows_read += 1
            raw_row_id = insert_raw_sheet_row_if_new(
                sheet_name=sheet_data["sheet_name"],
                row_number=row["sheet_row_number"],
                row_hash=row["row_hash"],
                raw_json=_build_raw_sheet_payload(row),
                case_id=row["case_id"],
                imported_at=imported_at,
                is_latest=True,
                connection=conn,
            )
            if raw_row_id is not None:
                rows_written += 1

            if not row["is_valid"]:
                invalid_rows += 1
                logger.warning(
                    "Skipping invalid master row %s: missing %s",
                    row["sheet_row_number"],
                    ", ".join(
                        REQUIRED_FIELD_LABELS[field]
                        for field in row["missing_required_fields"]
                    ),
                )
                continue

            result = upsert_case_from_sheet_row(
                sheet_name=sheet_data["sheet_name"],
                row=row,
                imported_at=imported_at,
                connection=conn,
            )
            rows_written += (
                result["case_changed"]
                + result["version_inserted"]
                + result["item_written"]
            )

        sheet_hash = compute_sheet_hash(sheet_data["rows"])
        upsert_sheet_sync_state(
            sheet_name=sheet_data["sheet_name"],
            last_sync_at=imported_at,
            last_seen_row_count=len(sheet_data["rows"]),
            last_sheet_hash=sheet_hash,
            connection=conn,
        )
        finish_import(
            import_id=import_id,
            status="success",
            rows_read=rows_read,
            rows_written=rows_written,
            connection=conn,
        )
        conn.commit()

        summary = {
            "import_id": import_id,
            "sheet_name": sheet_data["sheet_name"],
            "rows_read": rows_read,
            "rows_written": rows_written,
            "invalid_rows": invalid_rows,
            "case_id_updates": sheet_data["case_id_updates"],
            "last_sheet_hash": sheet_hash,
        }
        logger.info(
            "Master sheet sync finished: sheet=%s rows=%s writes=%s invalid=%s case_id_updates=%s",
            summary["sheet_name"],
            summary["rows_read"],
            summary["rows_written"],
            summary["invalid_rows"],
            summary["case_id_updates"],
        )
        return summary
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
