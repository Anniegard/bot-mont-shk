from __future__ import annotations

import hashlib
import json
import math
import os
import sqlite3
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterator

PROJECT_ROOT = Path(__file__).resolve().parent.parent
DEFAULT_DB_RELATIVE_PATH = Path("data") / "bot.db"
_EMPTY_TEXT_VALUES = {"", "none", "null", "nan"}

SCHEMA_STATEMENTS = (
    """
    CREATE TABLE IF NOT EXISTS cases (
        case_id TEXT PRIMARY KEY,
        source_sheet_name TEXT,
        sheet_row_number INTEGER,
        is_active INTEGER NOT NULL DEFAULT 1,
        created_at TEXT NOT NULL,
        updated_at TEXT NOT NULL
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS case_versions (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        case_id TEXT NOT NULL,
        row_hash TEXT NOT NULL,
        sheet_row_number INTEGER,
        raw_snapshot_json TEXT NOT NULL,
        imported_at TEXT NOT NULL,
        FOREIGN KEY (case_id) REFERENCES cases(case_id)
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS case_items (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        case_id TEXT NOT NULL,
        shk TEXT,
        tare_transfer TEXT,
        item_name TEXT,
        amount REAL,
        qty_shk INTEGER,
        last_movement_at TEXT,
        writeoff_started_at TEXT,
        example_related_shk TEXT,
        created_at TEXT NOT NULL,
        FOREIGN KEY (case_id) REFERENCES cases(case_id)
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS raw_sheet_rows (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        sheet_name TEXT NOT NULL,
        row_number INTEGER NOT NULL,
        case_id TEXT,
        row_hash TEXT NOT NULL,
        raw_json TEXT NOT NULL,
        imported_at TEXT NOT NULL,
        is_latest INTEGER NOT NULL DEFAULT 1
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS raw_yadisk_rows (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        import_batch_id INTEGER,
        source_file_name TEXT,
        source_path TEXT,
        row_hash TEXT NOT NULL,
        shk TEXT,
        tare_transfer TEXT,
        item_name TEXT,
        amount REAL,
        qty_shk INTEGER,
        last_movement_at TEXT,
        writeoff_started_at TEXT,
        example_related_shk TEXT,
        matched_case_id TEXT,
        match_method TEXT,
        match_confidence TEXT,
        imported_at TEXT NOT NULL
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS imports (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        source_type TEXT NOT NULL,
        source_name TEXT,
        source_path TEXT,
        sheet_name TEXT,
        status TEXT NOT NULL,
        rows_read INTEGER NOT NULL DEFAULT 0,
        rows_written INTEGER NOT NULL DEFAULT 0,
        started_at TEXT NOT NULL,
        finished_at TEXT,
        error_text TEXT
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS sheet_sync_state (
        sheet_name TEXT PRIMARY KEY,
        last_sync_at TEXT,
        last_seen_row_count INTEGER,
        last_sheet_hash TEXT
    )
    """,
)

INDEX_STATEMENTS = (
    "CREATE INDEX IF NOT EXISTS idx_case_versions_case_id ON case_versions(case_id)",
    "CREATE INDEX IF NOT EXISTS idx_case_versions_row_hash ON case_versions(row_hash)",
    "CREATE INDEX IF NOT EXISTS idx_case_items_case_id ON case_items(case_id)",
    "CREATE INDEX IF NOT EXISTS idx_case_items_shk ON case_items(shk)",
    "CREATE INDEX IF NOT EXISTS idx_case_items_tare_transfer ON case_items(tare_transfer)",
    "CREATE INDEX IF NOT EXISTS idx_case_items_item_name ON case_items(item_name)",
    "CREATE INDEX IF NOT EXISTS idx_raw_sheet_rows_sheet_name ON raw_sheet_rows(sheet_name)",
    "CREATE INDEX IF NOT EXISTS idx_raw_sheet_rows_case_id ON raw_sheet_rows(case_id)",
    "CREATE INDEX IF NOT EXISTS idx_raw_sheet_rows_row_hash ON raw_sheet_rows(row_hash)",
    "CREATE INDEX IF NOT EXISTS idx_raw_yadisk_rows_row_hash ON raw_yadisk_rows(row_hash)",
    "CREATE INDEX IF NOT EXISTS idx_raw_yadisk_rows_shk ON raw_yadisk_rows(shk)",
    "CREATE INDEX IF NOT EXISTS idx_raw_yadisk_rows_tare_transfer ON raw_yadisk_rows(tare_transfer)",
    "CREATE INDEX IF NOT EXISTS idx_raw_yadisk_rows_item_name ON raw_yadisk_rows(item_name)",
    "CREATE INDEX IF NOT EXISTS idx_raw_yadisk_rows_matched_case_id ON raw_yadisk_rows(matched_case_id)",
)


def resolve_db_path(db_path: str | Path | None = None) -> Path:
    configured_path = db_path
    if configured_path is None or str(configured_path).strip() == "":
        configured_path = os.getenv("BOT_DB_PATH") or DEFAULT_DB_RELATIVE_PATH

    path = Path(configured_path)
    if not path.is_absolute():
        path = (PROJECT_ROOT / path).resolve()
    path.parent.mkdir(parents=True, exist_ok=True)
    return path


def normalize_empty_value(value: Any) -> Any:
    if value is None:
        return None
    if isinstance(value, float) and math.isnan(value):
        return None
    if isinstance(value, str):
        stripped = value.strip()
        if stripped.lower() in _EMPTY_TEXT_VALUES:
            return None
        return stripped or None
    return value


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def serialize_json(value: Any) -> str:
    normalized = {} if value is None else value
    return json.dumps(normalized, ensure_ascii=False, sort_keys=True, default=str)


def calculate_row_hash(value: Any) -> str:
    payload = serialize_json(value)
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def _configure_connection(conn: sqlite3.Connection) -> sqlite3.Connection:
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON")
    try:
        conn.execute("PRAGMA journal_mode = WAL").fetchone()
    except sqlite3.DatabaseError:
        pass
    return conn


def get_db_connection(db_path: str | Path | None = None) -> sqlite3.Connection:
    resolved_path = resolve_db_path(db_path)
    connection = sqlite3.connect(resolved_path, timeout=30)
    return _configure_connection(connection)


@contextmanager
def _managed_connection(
    connection: sqlite3.Connection | None = None,
    db_path: str | Path | None = None,
) -> Iterator[sqlite3.Connection]:
    owns_connection = connection is None
    conn = connection or get_db_connection(db_path)
    try:
        yield conn
        if owns_connection:
            conn.commit()
    except Exception:
        if owns_connection:
            conn.rollback()
        raise
    finally:
        if owns_connection:
            conn.close()


def init_db(db_path: str | Path | None = None) -> Path:
    resolved_path = resolve_db_path(db_path)
    with _managed_connection(db_path=resolved_path) as conn:
        for statement in SCHEMA_STATEMENTS:
            conn.execute(statement)
        for statement in INDEX_STATEMENTS:
            conn.execute(statement)
    return resolved_path


def insert_import(
    source_type: str,
    source_name: str | None = None,
    source_path: str | None = None,
    sheet_name: str | None = None,
    status: str = "started",
    rows_read: int = 0,
    rows_written: int = 0,
    started_at: str | None = None,
    connection: sqlite3.Connection | None = None,
    db_path: str | Path | None = None,
) -> int:
    normalized_source_type = normalize_empty_value(source_type)
    normalized_status = normalize_empty_value(status)
    if not normalized_source_type:
        raise ValueError("source_type is required")
    if not normalized_status:
        raise ValueError("status is required")

    started = started_at or utc_now_iso()
    with _managed_connection(connection, db_path) as conn:
        cursor = conn.execute(
            """
            INSERT INTO imports (
                source_type,
                source_name,
                source_path,
                sheet_name,
                status,
                rows_read,
                rows_written,
                started_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                normalized_source_type,
                normalize_empty_value(source_name),
                normalize_empty_value(source_path),
                normalize_empty_value(sheet_name),
                normalized_status,
                int(rows_read or 0),
                int(rows_written or 0),
                started,
            ),
        )
        return int(cursor.lastrowid)


def finish_import(
    import_id: int,
    status: str,
    rows_read: int | None = None,
    rows_written: int | None = None,
    finished_at: str | None = None,
    error_text: str | None = None,
    connection: sqlite3.Connection | None = None,
    db_path: str | Path | None = None,
) -> None:
    normalized_status = normalize_empty_value(status)
    if not normalized_status:
        raise ValueError("status is required")

    update_fields = ["status = ?", "finished_at = ?", "error_text = ?"]
    params: list[Any] = [
        normalized_status,
        finished_at or utc_now_iso(),
        normalize_empty_value(error_text),
    ]
    if rows_read is not None:
        update_fields.append("rows_read = ?")
        params.append(int(rows_read))
    if rows_written is not None:
        update_fields.append("rows_written = ?")
        params.append(int(rows_written))
    params.append(int(import_id))

    with _managed_connection(connection, db_path) as conn:
        conn.execute(
            f"UPDATE imports SET {', '.join(update_fields)} WHERE id = ?",
            params,
        )


def upsert_case(
    case_id: str,
    source_sheet_name: str | None = None,
    sheet_row_number: int | None = None,
    is_active: bool | int = True,
    created_at: str | None = None,
    updated_at: str | None = None,
    connection: sqlite3.Connection | None = None,
    db_path: str | Path | None = None,
) -> str:
    normalized_case_id = normalize_empty_value(case_id)
    if not normalized_case_id:
        raise ValueError("case_id is required")

    created = created_at or utc_now_iso()
    updated = updated_at or created

    with _managed_connection(connection, db_path) as conn:
        conn.execute(
            """
            INSERT INTO cases (
                case_id,
                source_sheet_name,
                sheet_row_number,
                is_active,
                created_at,
                updated_at
            ) VALUES (?, ?, ?, ?, ?, ?)
            ON CONFLICT(case_id) DO UPDATE SET
                source_sheet_name = COALESCE(excluded.source_sheet_name, cases.source_sheet_name),
                sheet_row_number = COALESCE(excluded.sheet_row_number, cases.sheet_row_number),
                is_active = excluded.is_active,
                updated_at = excluded.updated_at
            """,
            (
                normalized_case_id,
                normalize_empty_value(source_sheet_name),
                sheet_row_number,
                1 if bool(is_active) else 0,
                created,
                updated,
            ),
        )
    return str(normalized_case_id)


def insert_case_version(
    case_id: str,
    row_hash: str,
    raw_snapshot_json: Any,
    sheet_row_number: int | None = None,
    imported_at: str | None = None,
    connection: sqlite3.Connection | None = None,
    db_path: str | Path | None = None,
) -> int:
    normalized_case_id = normalize_empty_value(case_id)
    normalized_row_hash = normalize_empty_value(row_hash) or calculate_row_hash(
        raw_snapshot_json
    )
    if not normalized_case_id:
        raise ValueError("case_id is required")

    with _managed_connection(connection, db_path) as conn:
        cursor = conn.execute(
            """
            INSERT INTO case_versions (
                case_id,
                row_hash,
                sheet_row_number,
                raw_snapshot_json,
                imported_at
            ) VALUES (?, ?, ?, ?, ?)
            """,
            (
                normalized_case_id,
                normalized_row_hash,
                sheet_row_number,
                serialize_json(raw_snapshot_json),
                imported_at or utc_now_iso(),
            ),
        )
        return int(cursor.lastrowid)


def insert_case_item(
    case_id: str,
    shk: str | None = None,
    tare_transfer: str | None = None,
    item_name: str | None = None,
    amount: float | int | None = None,
    qty_shk: int | None = None,
    last_movement_at: str | None = None,
    writeoff_started_at: str | None = None,
    example_related_shk: str | None = None,
    created_at: str | None = None,
    connection: sqlite3.Connection | None = None,
    db_path: str | Path | None = None,
) -> int:
    normalized_case_id = normalize_empty_value(case_id)
    if not normalized_case_id:
        raise ValueError("case_id is required")

    with _managed_connection(connection, db_path) as conn:
        cursor = conn.execute(
            """
            INSERT INTO case_items (
                case_id,
                shk,
                tare_transfer,
                item_name,
                amount,
                qty_shk,
                last_movement_at,
                writeoff_started_at,
                example_related_shk,
                created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                normalized_case_id,
                normalize_empty_value(shk),
                normalize_empty_value(tare_transfer),
                normalize_empty_value(item_name),
                amount,
                qty_shk,
                normalize_empty_value(last_movement_at),
                normalize_empty_value(writeoff_started_at),
                normalize_empty_value(example_related_shk),
                created_at or utc_now_iso(),
            ),
        )
        return int(cursor.lastrowid)


def insert_raw_sheet_row(
    sheet_name: str,
    row_number: int,
    row_hash: str,
    raw_json: Any,
    case_id: str | None = None,
    imported_at: str | None = None,
    is_latest: bool | int = True,
    connection: sqlite3.Connection | None = None,
    db_path: str | Path | None = None,
) -> int:
    normalized_sheet_name = normalize_empty_value(sheet_name)
    normalized_row_hash = normalize_empty_value(row_hash) or calculate_row_hash(raw_json)
    if not normalized_sheet_name:
        raise ValueError("sheet_name is required")

    with _managed_connection(connection, db_path) as conn:
        latest_flag = 1 if bool(is_latest) else 0
        if latest_flag:
            conn.execute(
                """
                UPDATE raw_sheet_rows
                SET is_latest = 0
                WHERE sheet_name = ? AND row_number = ? AND is_latest = 1
                """,
                (normalized_sheet_name, int(row_number)),
            )
        cursor = conn.execute(
            """
            INSERT INTO raw_sheet_rows (
                sheet_name,
                row_number,
                case_id,
                row_hash,
                raw_json,
                imported_at,
                is_latest
            ) VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (
                normalized_sheet_name,
                int(row_number),
                normalize_empty_value(case_id),
                normalized_row_hash,
                serialize_json(raw_json),
                imported_at or utc_now_iso(),
                latest_flag,
            ),
        )
        return int(cursor.lastrowid)


def insert_raw_yadisk_row(
    row_hash: str,
    import_batch_id: int | None = None,
    source_file_name: str | None = None,
    source_path: str | None = None,
    shk: str | None = None,
    tare_transfer: str | None = None,
    item_name: str | None = None,
    amount: float | int | None = None,
    qty_shk: int | None = None,
    last_movement_at: str | None = None,
    writeoff_started_at: str | None = None,
    example_related_shk: str | None = None,
    matched_case_id: str | None = None,
    match_method: str | None = None,
    match_confidence: str | None = None,
    imported_at: str | None = None,
    connection: sqlite3.Connection | None = None,
    db_path: str | Path | None = None,
) -> int:
    normalized_row_hash = normalize_empty_value(row_hash)
    if not normalized_row_hash:
        raise ValueError("row_hash is required")

    with _managed_connection(connection, db_path) as conn:
        cursor = conn.execute(
            """
            INSERT INTO raw_yadisk_rows (
                import_batch_id,
                source_file_name,
                source_path,
                row_hash,
                shk,
                tare_transfer,
                item_name,
                amount,
                qty_shk,
                last_movement_at,
                writeoff_started_at,
                example_related_shk,
                matched_case_id,
                match_method,
                match_confidence,
                imported_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                import_batch_id,
                normalize_empty_value(source_file_name),
                normalize_empty_value(source_path),
                normalized_row_hash,
                normalize_empty_value(shk),
                normalize_empty_value(tare_transfer),
                normalize_empty_value(item_name),
                amount,
                qty_shk,
                normalize_empty_value(last_movement_at),
                normalize_empty_value(writeoff_started_at),
                normalize_empty_value(example_related_shk),
                normalize_empty_value(matched_case_id),
                normalize_empty_value(match_method),
                normalize_empty_value(match_confidence),
                imported_at or utc_now_iso(),
            ),
        )
        return int(cursor.lastrowid)


def get_case_by_case_id(
    case_id: str,
    connection: sqlite3.Connection | None = None,
    db_path: str | Path | None = None,
) -> dict[str, Any] | None:
    normalized_case_id = normalize_empty_value(case_id)
    if not normalized_case_id:
        return None

    with _managed_connection(connection, db_path) as conn:
        row = conn.execute(
            "SELECT * FROM cases WHERE case_id = ?",
            (normalized_case_id,),
        ).fetchone()
        return dict(row) if row else None


def find_case_candidates(
    shk: str | None = None,
    tare_transfer: str | None = None,
    item_name: str | None = None,
    limit: int = 20,
    connection: sqlite3.Connection | None = None,
    db_path: str | Path | None = None,
) -> list[dict[str, Any]]:
    lookups = (
        ("shk", normalize_empty_value(shk), "high"),
        ("tare_transfer", normalize_empty_value(tare_transfer), "medium"),
        ("item_name", normalize_empty_value(item_name), "low"),
    )
    results: list[dict[str, Any]] = []
    seen_case_ids: set[str] = set()

    with _managed_connection(connection, db_path) as conn:
        for field_name, value, confidence in lookups:
            if not value or len(results) >= limit:
                continue

            if field_name == "item_name":
                query = """
                    SELECT
                        c.case_id,
                        c.source_sheet_name,
                        c.sheet_row_number,
                        c.is_active,
                        c.created_at,
                        c.updated_at,
                        ci.id AS case_item_id,
                        ci.shk,
                        ci.tare_transfer,
                        ci.item_name,
                        ci.amount,
                        ci.qty_shk,
                        ci.last_movement_at,
                        ci.writeoff_started_at,
                        ci.example_related_shk
                    FROM case_items AS ci
                    INNER JOIN cases AS c ON c.case_id = ci.case_id
                    WHERE c.is_active = 1 AND lower(trim(ci.item_name)) = lower(?)
                    ORDER BY c.updated_at DESC, ci.id DESC
                    LIMIT ?
                """
            else:
                query = f"""
                    SELECT
                        c.case_id,
                        c.source_sheet_name,
                        c.sheet_row_number,
                        c.is_active,
                        c.created_at,
                        c.updated_at,
                        ci.id AS case_item_id,
                        ci.shk,
                        ci.tare_transfer,
                        ci.item_name,
                        ci.amount,
                        ci.qty_shk,
                        ci.last_movement_at,
                        ci.writeoff_started_at,
                        ci.example_related_shk
                    FROM case_items AS ci
                    INNER JOIN cases AS c ON c.case_id = ci.case_id
                    WHERE c.is_active = 1 AND trim(ci.{field_name}) = ?
                    ORDER BY c.updated_at DESC, ci.id DESC
                    LIMIT ?
                """

            for row in conn.execute(query, (value, max(limit, 1))).fetchall():
                row_dict = dict(row)
                case_id_value = row_dict["case_id"]
                if case_id_value in seen_case_ids:
                    continue
                row_dict["match_method"] = field_name
                row_dict["match_confidence"] = confidence
                results.append(row_dict)
                seen_case_ids.add(case_id_value)
                if len(results) >= limit:
                    break

    return results
