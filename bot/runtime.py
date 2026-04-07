from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any

from bot.config import Config, load_config
from bot.db import init_db
from bot.logging_config import setup_logging
from bot.services.case_sync import sync_cases_from_master_sheet
from bot.services.sheets import authorize_client


@dataclass(frozen=True)
class AppRuntime:
    config: Config
    db_path: Path
    gspread_client: Any


def build_runtime(
    env_path: str | None = None,
    *,
    require_telegram_token: bool = True,
    require_web_auth: bool = False,
    sync_cases: bool = False,
) -> AppRuntime:
    config = load_config(
        env_path=env_path,
        require_telegram_token=require_telegram_token,
        require_web_auth=require_web_auth,
    )
    setup_logging(
        config.telegram_token,
        config.openai_api_key,
        config.yandex_oauth_token,
    )
    db_path = init_db(config.db_path)
    gspread_client = authorize_client(config.google_credentials_path)

    if sync_cases:
        sync_cases_from_master_sheet(
            client=gspread_client,
            spreadsheet_id=config.spreadsheet_id,
            db_path=db_path,
        )

    return AppRuntime(
        config=config,
        db_path=db_path,
        gspread_client=gspread_client,
    )
