from __future__ import annotations

import logging
from datetime import datetime
from pathlib import Path
from typing import List, Optional

import gspread
from google.oauth2.service_account import Credentials

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


def authorize_client(credentials_path: Path) -> gspread.Client:
    creds = Credentials.from_service_account_file(credentials_path, scopes=SCOPE)
    return gspread.authorize(creds)


def _get_worksheet(
    client: gspread.Client, spreadsheet_id: str, worksheet_name: Optional[str]
):
    spreadsheet = client.open_by_key(spreadsheet_id)
    if worksheet_name:
        try:
            return spreadsheet.worksheet(worksheet_name)
        except gspread.WorksheetNotFound:
            return spreadsheet.sheet1
    return spreadsheet.sheet1


def _format_meta_uploaded_at(meta: Optional[dict]) -> str:
    if not meta:
        return "нет данных"
    uploaded = meta.get("uploaded_at")
    if not uploaded:
        return "нет данных"
    try:
        dt = datetime.fromisoformat(uploaded)
        return dt.strftime("%d.%m.%Y %H:%M")
    except Exception:
        return str(uploaded)


def update_tables(
    client: gspread.Client,
    spreadsheet_id: str,
    worksheet_name: Optional[str],
    left_rows: List[List],
    right_rows: List[List],
    right_meta: Optional[dict],
    skip_left: bool = False,
    skip_right: bool = False,
) -> None:
    worksheet = _get_worksheet(client, spreadsheet_id, worksheet_name)

    start_row = 5  # rows 1-4 reserved (headers + empty row 4)

    left_existing = max(len(worksheet.col_values(2)) - (start_row - 1), 0)  # column B
    right_existing = max(len(worksheet.col_values(11)) - (start_row - 1), 0)  # column K

    clear_ranges = []
    if not skip_left and left_existing:
        clear_ranges.append(f"B{start_row}:E{start_row + left_existing - 1}")
    if not skip_right and right_existing:
        clear_ranges.append(f"K{start_row}:O{start_row + right_existing - 1}")
    if clear_ranges:
        worksheet.batch_clear(clear_ranges)

    updates = [
        {"range": "B2:E2", "values": [[LEFT_HEADER_TITLE, "", "", ""]]},
        {"range": "B3:E3", "values": [LEFT_COLUMNS]},
        {"range": "K2:O2", "values": [[RIGHT_HEADER_TITLE, "", "", "", ""]]},
        {"range": "K3:O3", "values": [RIGHT_COLUMNS]},
        {"range": "P3", "values": [[META_LABEL]]},
        {"range": "P4", "values": [[_format_meta_uploaded_at(right_meta)]]},
    ]

    if not skip_left and left_rows:
        updates.append(
            {
                "range": f"B{start_row}:E{start_row + len(left_rows) - 1}",
                "values": left_rows,
            }
        )
    if not skip_right and right_rows:
        updates.append(
            {
                "range": f"K{start_row}:O{start_row + len(right_rows) - 1}",
                "values": right_rows,
            }
        )

    worksheet.batch_update(updates)

    try:
        worksheet.merge_cells("B2:E2")
    except Exception:
        pass
    try:
        worksheet.merge_cells("K2:O2")
    except Exception:
        pass

    logging.info(
        "Таблицы обновлены: без движения строк=%s, 24ч строк=%s",
        len(left_rows),
        len(right_rows),
    )
