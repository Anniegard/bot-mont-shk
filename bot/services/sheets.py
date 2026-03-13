from __future__ import annotations

import logging
from datetime import datetime
from pathlib import Path
from typing import Any, List, Optional
from zoneinfo import ZoneInfo

import gspread
from google.oauth2.service_account import Credentials

from bot.constants import (
    CASE_ID_COLUMN_NAME,
    EXPORT_24H_SHEET_NAME,
    EXPORT_NO_MOVE_SHEET_NAME,
)

SCOPE = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
]

LEFT_HEADER_TITLE = "Выгрузка Идентификатор товара без движения"
LEFT_COLUMNS = ["Гофра", "Идентификатор товара", "Кол-во", "Стоимость"]

RIGHT_HEADER_TITLE = "Товар, который спишется в течение 24ч"
RIGHT_COLUMNS = [
    "ID тары",
    "Идентификатор товара",
    "Кол-во",
    "Стоимость",
    "Когда начнёт списываться?",
]
META_LABEL = "Актуальность файла 24ч:"
MOSCOW_TZ = ZoneInfo("Europe/Moscow")
UTC_TZ = ZoneInfo("UTC")
EXPORT_WORKSHEET_ROWS = 1000
EXPORT_WORKSHEET_COLS = 16


def _normalize_sheet_cell(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def get_case_id_column_index(headers: list[str]) -> int | None:
    for index, header in enumerate(headers):
        if str(header).strip().lower() == CASE_ID_COLUMN_NAME:
            return index
    return None


def parse_sheet_rows(values: list[list[Any]]) -> list[dict[str, Any]]:
    if not values:
        return []

    headers = [str(header).strip() for header in values[0]]
    case_id_index = get_case_id_column_index(headers)
    parsed_rows: list[dict[str, Any]] = []

    for row_number, row in enumerate(values[1:], start=2):
        padded_row = list(row[: len(headers)])
        if len(padded_row) < len(headers):
            padded_row.extend([""] * (len(headers) - len(padded_row)))

        raw_row = dict(zip(headers, padded_row))
        parsed_rows.append(
            {
                "row_number": row_number,
                "case_id": (
                    _normalize_sheet_cell(padded_row[case_id_index])
                    if case_id_index is not None and case_id_index < len(padded_row)
                    else None
                ),
                "raw_row": raw_row,
            }
        )

    return parsed_rows


def read_sheet_rows(
    client: gspread.Client, spreadsheet_id: str, worksheet_name: Optional[str]
) -> list[dict[str, Any]]:
    worksheet = get_worksheet(client, spreadsheet_id, worksheet_name)
    values = worksheet.get_all_values()
    return parse_sheet_rows(values)


def authorize_client(credentials_path: Path) -> gspread.Client:
    creds = Credentials.from_service_account_file(credentials_path, scopes=SCOPE)
    return gspread.authorize(creds)


def open_spreadsheet(client: gspread.Client, spreadsheet_id: str):
    return client.open_by_key(spreadsheet_id)


def get_worksheet(
    client: gspread.Client, spreadsheet_id: str, worksheet_name: Optional[str]
):
    spreadsheet = open_spreadsheet(client, spreadsheet_id)
    if worksheet_name:
        return spreadsheet.worksheet(worksheet_name)
    return spreadsheet.sheet1


def _get_worksheet(
    client: gspread.Client, spreadsheet_id: str, worksheet_name: Optional[str]
):
    return get_worksheet(client, spreadsheet_id, worksheet_name)


def _format_meta_uploaded_at(meta: Optional[dict]) -> str:
    if not meta:
        return "нет данных"
    uploaded = meta.get("uploaded_at")
    if not uploaded:
        return "нет данных"
    try:
        dt = datetime.fromisoformat(uploaded)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=UTC_TZ)
        dt = dt.astimezone(MOSCOW_TZ)
        return dt.strftime("%d.%m.%Y %H:%M")
    except Exception:
        return str(uploaded)


def get_or_create_worksheet(
    client: gspread.Client,
    spreadsheet_id: str,
    worksheet_name: str,
):
    spreadsheet = open_spreadsheet(client, spreadsheet_id)
    try:
        return spreadsheet.worksheet(worksheet_name)
    except gspread.WorksheetNotFound:
        return spreadsheet.add_worksheet(
            title=worksheet_name,
            rows=EXPORT_WORKSHEET_ROWS,
            cols=EXPORT_WORKSHEET_COLS,
        )


def _ensure_worksheet_columns(
    worksheet: gspread.Worksheet, required_columns: int
) -> None:
    if worksheet.col_count < required_columns:
        worksheet.add_cols(required_columns - worksheet.col_count)


def _update_no_move_export_tab(
    worksheet: gspread.Worksheet,
    left_rows: List[List],
) -> None:
    start_row = 5  # rows 1-4 reserved (headers + empty row 4)
    _ensure_worksheet_columns(worksheet, 5)

    left_existing = max(len(worksheet.col_values(2)) - (start_row - 1), 0)  # column B
    if left_existing:
        worksheet.batch_clear([f"B{start_row}:E{start_row + left_existing - 1}"])

    updates = [
        {"range": "B2:E2", "values": [[LEFT_HEADER_TITLE, "", "", ""]]},
        {"range": "B3:E3", "values": [LEFT_COLUMNS]},
    ]
    if left_rows:
        updates.append(
            {
                "range": f"B{start_row}:E{start_row + len(left_rows) - 1}",
                "values": left_rows,
            }
        )

    worksheet.batch_update(updates)

    try:
        worksheet.merge_cells("B2:E2")
    except Exception:
        pass


def _update_24h_export_tab(
    worksheet: gspread.Worksheet,
    right_rows: List[List],
    right_meta: Optional[dict],
) -> None:
    start_row = 5  # rows 1-4 reserved (headers + empty row 4)
    _ensure_worksheet_columns(worksheet, 16)

    right_existing = max(
        len(worksheet.col_values(11)) - (start_row - 1), 0
    )  # column K
    if right_existing:
        worksheet.batch_clear([f"K{start_row}:O{start_row + right_existing - 1}"])

    updates = [
        {"range": "K2:O2", "values": [[RIGHT_HEADER_TITLE, "", "", "", ""]]},
        {"range": "K3:O3", "values": [RIGHT_COLUMNS]},
        {"range": "P2", "values": [[META_LABEL]]},
        {"range": "P3", "values": [[_format_meta_uploaded_at(right_meta)]]},
    ]
    if right_rows:
        updates.append(
            {
                "range": f"K{start_row}:O{start_row + len(right_rows) - 1}",
                "values": right_rows,
            }
        )

    worksheet.batch_update(updates)

    try:
        worksheet.merge_cells("K2:O2")
    except Exception:
        pass


def update_tables(
    client: gspread.Client,
    spreadsheet_id: str,
    left_rows: List[List],
    right_rows: List[List],
    right_meta: Optional[dict],
    skip_left: bool = False,
    skip_right: bool = False,
) -> None:
    if not skip_left:
        no_move_worksheet = get_or_create_worksheet(
            client,
            spreadsheet_id,
            EXPORT_NO_MOVE_SHEET_NAME,
        )
        _update_no_move_export_tab(no_move_worksheet, left_rows)

    if not skip_right:
        export_24h_worksheet = get_or_create_worksheet(
            client,
            spreadsheet_id,
            EXPORT_24H_SHEET_NAME,
        )
        _update_24h_export_tab(export_24h_worksheet, right_rows, right_meta)

    logging.info(
        "Экспортные вкладки обновлены: no_move_sheet=%s rows=%s, export_24h_sheet=%s rows=%s",
        EXPORT_NO_MOVE_SHEET_NAME,
        len(left_rows),
        EXPORT_24H_SHEET_NAME,
        len(right_rows),
    )
