from __future__ import annotations

import logging
from pathlib import Path
from typing import List, Optional

import gspread
from google.oauth2.service_account import Credentials

SCOPE = ["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"]


def authorize_client(credentials_path: Path) -> gspread.Client:
    creds = Credentials.from_service_account_file(credentials_path, scopes=SCOPE)
    return gspread.authorize(creds)


def _get_worksheet(client: gspread.Client, spreadsheet_id: str, worksheet_name: Optional[str]):
    spreadsheet = client.open_by_key(spreadsheet_id)
    if worksheet_name:
        return spreadsheet.worksheet(worksheet_name)
    return spreadsheet.sheet1


def upload_to_google_sheets(
    rows: List[List],
    client: gspread.Client,
    spreadsheet_id: str,
    worksheet_name: Optional[str] = None,
) -> None:
    worksheet = _get_worksheet(client, spreadsheet_id, worksheet_name)

    existing_rows = len(worksheet.col_values(4))  # column D
    rows_to_clear = max(existing_rows, len(rows), 1)
    clear_range = f"D1:G{rows_to_clear}"
    worksheet.batch_clear([clear_range])

    if rows:
        update_range = f"D1:G{len(rows)}"
        worksheet.update(update_range, rows, value_input_option="RAW")
        logging.info(
            "Данные успешно загружены в Google Sheets. Строк: %s. Диапазон: %s. Лист: %s",
            len(rows),
            update_range,
            worksheet.title,
        )
    else:
        logging.info("Данные для загрузки отсутствуют. Диапазон %s очищен.", clear_range)
