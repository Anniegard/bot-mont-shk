from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path
from urllib.parse import urlsplit
from typing import Tuple

from dotenv import load_dotenv


@dataclass
class Config:
    telegram_token: str | None
    spreadsheet_id: str
    google_credentials_path: Path
    db_path: Path
    worksheet_name: str | None = None
    admin_user_id: str | None = None
    admin_user_ids: Tuple[str, ...] = ()
    yandex_oauth_token: str | None = None
    yandex_no_move_dir: str | None = None
    yandex_24h_dir: str | None = None
    yandex_warehouse_delay_dir: str | None = None
    yandex_allowed_exts: Tuple[str, ...] = (".xlsx", ".xls", ".zip")
    yandex_max_mb: int = 200
    warehouse_delay_worksheet_name: str | None = None
    public_base_url: str | None = None
    web_secret_key: str | None = None
    web_host: str = "127.0.0.1"
    web_port: int = 8000
    web_rate_limit_per_minute: int = 20
    web_max_upload_mb: int = 20
    # Trust proxy headers (x-real-ip / x-forwarded-for) only when explicitly enabled.
    web_trust_proxy_headers: bool = False
    web_admin_username: str | None = None
    web_admin_password: str | None = None
    web_user_username: str | None = None
    web_user_password: str | None = None
    telegram_webhook_url: str | None = None
    telegram_webhook_secret: str | None = None
    telegram_webhook_listen: str = "127.0.0.1"
    telegram_webhook_port: int = 8081
    telegram_webhook_path: str = "/tg/webhook"
    ai_enabled: bool = False
    ai_provider: str = "openai"
    ai_admin_user_ids: Tuple[str, ...] = ()
    openai_api_key: str | None = None
    openai_model: str | None = None
    openai_base_url: str | None = None
    openai_timeout_seconds: int = 30
    ai_max_concurrent_requests: int = 1
    ai_max_files_per_request: int = 3
    ai_max_file_mb: int = 10
    ai_max_rows_per_source: int = 200
    ai_max_scan_rows_per_source: int = 5000
    ai_max_context_chars: int = 12000
    ai_max_history_messages: int = 4
    ai_max_answer_chars: int = 1800
    ai_max_retries: int = 2
    ai_retry_backoff_ms: int = 750
    ai_temperature: float = 0.1
    ai_source_cache_ttl_seconds: int = 300


def _resolve_credentials_path(path_value: str, root_dir: Path) -> Path:
    path = Path(path_value)
    if not path.is_absolute():
        path = (root_dir / path_value).resolve()
    return path


def _resolve_optional_path(path_value: str, root_dir: Path) -> Path:
    path = Path(path_value)
    if not path.is_absolute():
        path = (root_dir / path_value).resolve()
    return path


def parse_admin_user_ids(
    bot_admin_ids: str | None = None,
    legacy_admin_user_id: str | None = None,
) -> Tuple[str, ...]:
    values: list[str] = []
    seen: set[str] = set()

    for raw_value in (bot_admin_ids or "").split(","):
        normalized = raw_value.strip()
        if normalized and normalized not in seen:
            values.append(normalized)
            seen.add(normalized)

    legacy_value = (legacy_admin_user_id or "").strip()
    if legacy_value and legacy_value not in seen:
        values.append(legacy_value)

    return tuple(values)


def _parse_bool_env(value: str | None) -> bool:
    """
    Parse typical boolean env values.

    Returns False for empty/unknown values to keep behaviour conservative.
    """
    if value is None:
        return False
    normalized = value.strip().lower()
    if normalized in {"1", "true", "yes", "y", "on"}:
        return True
    if normalized in {"0", "false", "no", "n", "off", ""}:
        return False
    return False


def _clamp_int_env(
    value: str | None,
    *,
    default: int,
    minimum: int,
    maximum: int,
) -> int:
    try:
        parsed = int(value or default)
    except (TypeError, ValueError):
        parsed = default
    return max(minimum, min(parsed, maximum))


def _clamp_float_env(
    value: str | None,
    *,
    default: float,
    minimum: float,
    maximum: float,
) -> float:
    try:
        parsed = float(value or default)
    except (TypeError, ValueError):
        parsed = default
    return max(minimum, min(parsed, maximum))


def _normalize_webhook_path(path_value: str | None) -> str:
    normalized = (path_value or "").strip() or "/tg/webhook"
    if not normalized.startswith("/"):
        normalized = f"/{normalized}"
    return normalized.rstrip("/") or "/"


def _extract_webhook_path(url_value: str | None) -> str | None:
    if not url_value:
        return None
    try:
        parsed = urlsplit(url_value)
    except Exception:
        return None
    return parsed.path or None


def load_config(
    env_path: str | None = None,
    *,
    require_telegram_token: bool = True,
    require_web_auth: bool = False,
) -> Config:
    """
    Load configuration from environment (optionally from .env).
    Mandatory: TELEGRAM_TOKEN (or TELEGRAM_BOT_TOKEN), SPREADSHEET_ID, GOOGLE_CREDENTIALS_PATH.
    Optional: WORKSHEET_NAME, ADMIN_USER_ID, BOT_DB_PATH.
    """
    root_dir = Path(__file__).resolve().parent.parent
    env_override = os.getenv("BOT_CONFIG_FILE")
    env_file = (
        Path(env_path or env_override)
        if (env_path or env_override)
        else root_dir / ".env"
    )
    if env_file and not env_file.is_absolute():
        env_file = (root_dir / env_file).resolve()
    if env_file.exists():
        load_dotenv(env_file, override=False)
    elif env_path or env_override:
        pass
    else:
        load_dotenv(override=False)

    telegram_token = (
        os.getenv("TELEGRAM_TOKEN") or os.getenv("TELEGRAM_BOT_TOKEN") or ""
    )
    spreadsheet_id = os.getenv("SPREADSHEET_ID", "")
    credentials_path_value = os.getenv("GOOGLE_CREDENTIALS_PATH", "")
    db_path_value = os.getenv("BOT_DB_PATH") or "data/bot.db"
    worksheet_name = os.getenv("WORKSHEET_NAME")
    admin_user_id = os.getenv("ADMIN_USER_ID") or None
    admin_user_ids = parse_admin_user_ids(
        bot_admin_ids=os.getenv("BOT_ADMIN_IDS"),
        legacy_admin_user_id=admin_user_id,
    )
    yandex_oauth_token = os.getenv("YANDEX_OAUTH_TOKEN") or None
    yandex_no_move_dir = os.getenv("YANDEX_NO_MOVE_DIR") or "/BOT_UPLOADS/no_move/"
    yandex_24h_dir = os.getenv("YANDEX_24H_DIR") or "/BOT_UPLOADS/24h/"
    yandex_warehouse_delay_dir = (
        os.getenv("YANDEX_WAREHOUSE_DELAY_DIR") or "disk:/BOT_UPLOADS/warehouse_delay/"
    )
    yandex_allowed_exts = tuple(
        e.strip()
        for e in (os.getenv("YANDEX_ALLOWED_EXTS") or ".xlsx,.xls,.zip").split(",")
        if e.strip()
    )
    yandex_max_mb = int(os.getenv("YANDEX_MAX_MB") or 200)
    warehouse_delay_worksheet_name = (
        os.getenv("WAREHOUSE_DELAY_WORKSHEET_NAME") or "Выгрузка задержка склада"
    )
    public_base_url = os.getenv("PUBLIC_BASE_URL") or None
    web_secret_key = os.getenv("WEB_SECRET_KEY") or None
    web_host = os.getenv("WEB_HOST") or "127.0.0.1"
    web_port = int(os.getenv("WEB_PORT") or 8000)
    web_rate_limit_per_minute = int(os.getenv("WEB_RATE_LIMIT_PER_MINUTE") or 20)
    web_max_upload_mb = int(os.getenv("WEB_MAX_UPLOAD_MB") or 20)
    web_trust_proxy_headers = _parse_bool_env(
        os.getenv("WEB_TRUST_PROXY_HEADERS")
    )
    web_admin_username = os.getenv("WEB_ADMIN_USERNAME") or None
    web_admin_password = os.getenv("WEB_ADMIN_PASSWORD") or None
    web_user_username = os.getenv("WEB_USER_USERNAME") or None
    web_user_password = os.getenv("WEB_USER_PASSWORD") or None
    telegram_webhook_url = os.getenv("TELEGRAM_WEBHOOK_URL") or None
    telegram_webhook_secret = os.getenv("TELEGRAM_WEBHOOK_SECRET") or None
    telegram_webhook_listen = os.getenv("TELEGRAM_WEBHOOK_LISTEN") or "127.0.0.1"
    telegram_webhook_port = int(os.getenv("TELEGRAM_WEBHOOK_PORT") or 8081)
    telegram_webhook_path = _normalize_webhook_path(
        os.getenv("TELEGRAM_WEBHOOK_PATH") or _extract_webhook_path(telegram_webhook_url)
    )
    webhook_mode_enabled = bool(telegram_webhook_url)
    ai_enabled = _parse_bool_env(os.getenv("AI_ENABLED"))
    ai_provider = (os.getenv("AI_PROVIDER") or "openai").strip().lower() or "openai"
    ai_admin_user_ids = parse_admin_user_ids(
        bot_admin_ids=os.getenv("AI_ADMIN_IDS"),
        legacy_admin_user_id=None,
    ) or admin_user_ids
    openai_api_key = os.getenv("OPENAI_API_KEY") or None
    openai_model = os.getenv("OPENAI_MODEL") or None
    openai_base_url = (os.getenv("OPENAI_BASE_URL") or "").strip() or None
    openai_timeout_seconds = _clamp_int_env(
        os.getenv("OPENAI_TIMEOUT_SECONDS"),
        default=30,
        minimum=5,
        maximum=180,
    )
    ai_max_concurrent_requests = _clamp_int_env(
        os.getenv("AI_MAX_CONCURRENT_REQUESTS"),
        default=1,
        minimum=1,
        maximum=5,
    )
    ai_max_files_per_request = _clamp_int_env(
        os.getenv("AI_MAX_FILES_PER_REQUEST"),
        default=3,
        minimum=1,
        maximum=10,
    )
    ai_max_file_mb = _clamp_int_env(
        os.getenv("AI_MAX_FILE_MB"),
        default=10,
        minimum=1,
        maximum=50,
    )
    ai_max_rows_per_source = _clamp_int_env(
        os.getenv("AI_MAX_ROWS_PER_SOURCE"),
        default=200,
        minimum=20,
        maximum=1000,
    )
    ai_max_scan_rows_per_source = _clamp_int_env(
        os.getenv("AI_MAX_SCAN_ROWS_PER_SOURCE"),
        default=5000,
        minimum=100,
        maximum=50000,
    )
    ai_max_context_chars = _clamp_int_env(
        os.getenv("AI_MAX_CONTEXT_CHARS"),
        default=12000,
        minimum=2000,
        maximum=40000,
    )
    ai_max_history_messages = _clamp_int_env(
        os.getenv("AI_MAX_HISTORY_MESSAGES"),
        default=4,
        minimum=0,
        maximum=20,
    )
    ai_max_answer_chars = _clamp_int_env(
        os.getenv("AI_MAX_ANSWER_CHARS"),
        default=1800,
        minimum=300,
        maximum=4000,
    )
    ai_max_retries = _clamp_int_env(
        os.getenv("AI_MAX_RETRIES"),
        default=2,
        minimum=0,
        maximum=5,
    )
    ai_retry_backoff_ms = _clamp_int_env(
        os.getenv("AI_RETRY_BACKOFF_MS"),
        default=750,
        minimum=100,
        maximum=5000,
    )
    ai_temperature = _clamp_float_env(
        os.getenv("AI_TEMPERATURE"),
        default=0.1,
        minimum=0.0,
        maximum=1.0,
    )
    ai_source_cache_ttl_seconds = _clamp_int_env(
        os.getenv("AI_SOURCE_CACHE_TTL_SECONDS"),
        default=300,
        minimum=0,
        maximum=3600,
    )

    missing = []
    if require_telegram_token and not telegram_token:
        missing.append("TELEGRAM_TOKEN (or TELEGRAM_BOT_TOKEN)")
    if not spreadsheet_id:
        missing.append("SPREADSHEET_ID")
    if not credentials_path_value:
        missing.append("GOOGLE_CREDENTIALS_PATH")
    if require_web_auth and not web_secret_key:
        missing.append("WEB_SECRET_KEY")
    if require_web_auth and not web_admin_username:
        missing.append("WEB_ADMIN_USERNAME")
    if require_web_auth and not web_admin_password:
        missing.append("WEB_ADMIN_PASSWORD")
    if require_web_auth and not web_user_username:
        missing.append("WEB_USER_USERNAME")
    if require_web_auth and not web_user_password:
        missing.append("WEB_USER_PASSWORD")
    if webhook_mode_enabled and not telegram_webhook_secret:
        missing.append("TELEGRAM_WEBHOOK_SECRET")

    if missing:
        raise ValueError(
            f"Missing required environment variables: {', '.join(missing)}"
        )

    credentials_path = _resolve_credentials_path(credentials_path_value, root_dir)
    if not credentials_path.exists():
        raise ValueError(f"GOOGLE_CREDENTIALS_PATH does not exist: {credentials_path}")
    db_path = _resolve_optional_path(db_path_value, root_dir)
    db_path.parent.mkdir(parents=True, exist_ok=True)

    return Config(
        telegram_token=telegram_token,
        spreadsheet_id=spreadsheet_id,
        google_credentials_path=credentials_path,
        db_path=db_path,
        worksheet_name=worksheet_name or None,
        admin_user_id=admin_user_id,
        admin_user_ids=admin_user_ids,
        yandex_oauth_token=yandex_oauth_token,
        yandex_no_move_dir=yandex_no_move_dir,
        yandex_24h_dir=yandex_24h_dir,
        yandex_warehouse_delay_dir=yandex_warehouse_delay_dir,
        yandex_allowed_exts=yandex_allowed_exts,
        yandex_max_mb=yandex_max_mb,
        warehouse_delay_worksheet_name=warehouse_delay_worksheet_name,
        public_base_url=public_base_url,
        web_secret_key=web_secret_key,
        web_host=web_host,
        web_port=web_port,
        web_rate_limit_per_minute=web_rate_limit_per_minute,
        web_max_upload_mb=web_max_upload_mb,
        web_trust_proxy_headers=web_trust_proxy_headers,
        web_admin_username=web_admin_username,
        web_admin_password=web_admin_password,
        web_user_username=web_user_username,
        web_user_password=web_user_password,
        telegram_webhook_url=telegram_webhook_url,
        telegram_webhook_secret=telegram_webhook_secret,
        telegram_webhook_listen=telegram_webhook_listen,
        telegram_webhook_port=telegram_webhook_port,
        telegram_webhook_path=telegram_webhook_path,
        ai_enabled=ai_enabled,
        ai_provider=ai_provider,
        ai_admin_user_ids=ai_admin_user_ids,
        openai_api_key=openai_api_key,
        openai_model=openai_model,
        openai_base_url=openai_base_url,
        openai_timeout_seconds=openai_timeout_seconds,
        ai_max_concurrent_requests=ai_max_concurrent_requests,
        ai_max_files_per_request=ai_max_files_per_request,
        ai_max_file_mb=ai_max_file_mb,
        ai_max_rows_per_source=ai_max_rows_per_source,
        ai_max_scan_rows_per_source=ai_max_scan_rows_per_source,
        ai_max_context_chars=ai_max_context_chars,
        ai_max_history_messages=ai_max_history_messages,
        ai_max_answer_chars=ai_max_answer_chars,
        ai_max_retries=ai_max_retries,
        ai_retry_backoff_ms=ai_retry_backoff_ms,
        ai_temperature=ai_temperature,
        ai_source_cache_ttl_seconds=ai_source_cache_ttl_seconds,
    )
