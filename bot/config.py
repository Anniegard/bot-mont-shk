from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path
from typing import Tuple

from dotenv import load_dotenv


@dataclass
class Config:
    telegram_token: str
    spreadsheet_id: str
    google_credentials_path: Path
    worksheet_name: str | None = None
    admin_user_id: str | None = None
    yandex_oauth_token: str | None = None
    yandex_no_move_dir: str | None = None
    yandex_24h_dir: str | None = None
    yandex_allowed_exts: Tuple[str, ...] = (".xlsx", ".xls", ".zip")
    yandex_max_mb: int = 200


def _resolve_credentials_path(path_value: str, root_dir: Path) -> Path:
    path = Path(path_value)
    if not path.is_absolute():
        path = (root_dir / path_value).resolve()
    return path


def load_config(env_path: str | None = None) -> Config:
    """
    Load configuration from environment (optionally from .env).
    Mandatory: TELEGRAM_TOKEN (or TELEGRAM_BOT_TOKEN), SPREADSHEET_ID, GOOGLE_CREDENTIALS_PATH.
    Optional: WORKSHEET_NAME, ADMIN_USER_ID.
    """
    root_dir = Path(__file__).resolve().parent.parent
    env_override = os.getenv("BOT_CONFIG_FILE")
    env_file = Path(env_path or env_override) if (env_path or env_override) else root_dir / ".env"
    if env_file and not env_file.is_absolute():
        env_file = (root_dir / env_file).resolve()
    if env_file.exists():
        load_dotenv(env_file, override=False)
    else:
        # Fallback to default behaviour (environment-only) if .env is absent
        load_dotenv(override=False)

    telegram_token = os.getenv("TELEGRAM_TOKEN") or os.getenv("TELEGRAM_BOT_TOKEN") or ""
    spreadsheet_id = os.getenv("SPREADSHEET_ID", "")
    credentials_path_value = os.getenv("GOOGLE_CREDENTIALS_PATH", "")
    worksheet_name = os.getenv("WORKSHEET_NAME")
    admin_user_id = os.getenv("ADMIN_USER_ID") or None
    yandex_oauth_token = os.getenv("YANDEX_OAUTH_TOKEN") or None
    yandex_no_move_dir = os.getenv("YANDEX_NO_MOVE_DIR") or "/BOT_UPLOADS/no_move/"
    yandex_24h_dir = os.getenv("YANDEX_24H_DIR") or "/BOT_UPLOADS/24h/"
    yandex_allowed_exts = tuple(
        e.strip() for e in (os.getenv("YANDEX_ALLOWED_EXTS") or ".xlsx,.xls,.zip").split(",") if e.strip()
    )
    yandex_max_mb = int(os.getenv("YANDEX_MAX_MB") or 200)

    missing = []
    if not telegram_token:
        missing.append("TELEGRAM_TOKEN (or TELEGRAM_BOT_TOKEN)")
    if not spreadsheet_id:
        missing.append("SPREADSHEET_ID")
    if not credentials_path_value:
        missing.append("GOOGLE_CREDENTIALS_PATH")

    if missing:
        raise ValueError(f"Missing required environment variables: {', '.join(missing)}")

    credentials_path = _resolve_credentials_path(credentials_path_value, root_dir)
    if not credentials_path.exists():
        raise ValueError(f"GOOGLE_CREDENTIALS_PATH does not exist: {credentials_path}")

    return Config(
        telegram_token=telegram_token,
        spreadsheet_id=spreadsheet_id,
        google_credentials_path=credentials_path,
        worksheet_name=worksheet_name or None,
        admin_user_id=admin_user_id,
        yandex_oauth_token=yandex_oauth_token,
        yandex_no_move_dir=yandex_no_move_dir,
        yandex_24h_dir=yandex_24h_dir,
        yandex_allowed_exts=yandex_allowed_exts,
        yandex_max_mb=yandex_max_mb,
    )
