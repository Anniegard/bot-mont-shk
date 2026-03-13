from __future__ import annotations

import sqlite3
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Iterator

from bot.db import (
    find_case_candidates_by_item_name,
    find_case_candidates_by_shk,
    find_case_candidates_by_tare_transfer,
    get_case_by_case_id,
    get_db_connection,
    get_raw_yadisk_row,
    normalize_empty_value,
    utc_now_iso,
)

REVIEW_STATUS_PENDING = "pending"
REVIEW_STATUS_LINKED = "linked"
REVIEW_STATUS_IGNORED = "ignored"


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


def _normalize_limit(limit: int | None, default: int) -> int:
    try:
        return max(1, int(limit or default))
    except (TypeError, ValueError):
        return default


def _insert_review_action(
    conn: sqlite3.Connection,
    *,
    raw_row_id: int,
    action: str,
    previous_case_id: str | None,
    new_case_id: str | None,
    actor_id: str | None,
    note: str | None,
    created_at: str,
) -> None:
    conn.execute(
        """
        INSERT INTO raw_review_actions (
            raw_row_id,
            action,
            previous_case_id,
            new_case_id,
            actor_id,
            note,
            created_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?)
        """,
        (
            int(raw_row_id),
            action,
            normalize_empty_value(previous_case_id),
            normalize_empty_value(new_case_id),
            normalize_empty_value(actor_id),
            normalize_empty_value(note),
            created_at,
        ),
    )


def _update_raw_row(
    conn: sqlite3.Connection,
    *,
    raw_row_id: int,
    updates: dict[str, Any],
) -> None:
    assignments = ", ".join(f"{column_name} = ?" for column_name in updates)
    conn.execute(
        f"UPDATE raw_yadisk_rows SET {assignments} WHERE id = ?",
        [*updates.values(), int(raw_row_id)],
    )


def list_unresolved_raw_rows(
    limit: int = 20,
    source_kind: str | None = None,
    include_ambiguous: bool = True,
    connection: sqlite3.Connection | None = None,
    db_path: str | Path | None = None,
) -> list[dict[str, Any]]:
    normalized_source_kind = normalize_empty_value(source_kind)
    normalized_limit = _normalize_limit(limit, 20)
    confidence_values = ["low", "none", "manual"]
    if include_ambiguous:
        confidence_values.insert(0, "ambiguous")
    placeholders = ", ".join("?" for _ in confidence_values)

    where_clauses = [
        "review_status = ?",
        (
            "(\n"
            "    matched_case_id IS NULL\n"
            f"    OR ifnull(match_confidence, 'none') IN ({placeholders})\n"
            ")"
        ),
    ]
    params: list[Any] = [REVIEW_STATUS_PENDING, *confidence_values]

    if normalized_source_kind:
        where_clauses.append("source_kind = ?")
        params.append(normalized_source_kind)

    params.append(normalized_limit)
    with _managed_connection(connection, db_path) as conn:
        rows = conn.execute(
            f"""
            SELECT
                id,
                source_kind,
                source_sheet_name,
                source_row_number,
                source_file_name,
                source_path,
                shk,
                tare_transfer,
                item_name,
                matched_case_id,
                match_method,
                match_confidence,
                review_status,
                review_note,
                imported_at
            FROM raw_yadisk_rows
            WHERE {' AND '.join(where_clauses)}
            ORDER BY imported_at DESC, id DESC
            LIMIT ?
            """,
            params,
        ).fetchall()
        return [dict(row) for row in rows]


def get_raw_row_details(
    raw_row_id: int,
    connection: sqlite3.Connection | None = None,
    db_path: str | Path | None = None,
) -> dict[str, Any] | None:
    with _managed_connection(connection, db_path) as conn:
        row = conn.execute(
            """
            SELECT
                id,
                source_kind,
                source_sheet_name,
                source_row_number,
                source_file_name,
                source_path,
                shk,
                tare_transfer,
                item_name,
                amount,
                qty_shk,
                last_movement_at,
                writeoff_started_at,
                matched_case_id,
                match_method,
                match_confidence,
                review_status,
                review_note,
                reviewed_at,
                reviewed_by,
                manual_linked_at,
                imported_at,
                link_decision_reason
            FROM raw_yadisk_rows
            WHERE id = ?
            LIMIT 1
            """,
            (int(raw_row_id),),
        ).fetchone()
        return dict(row) if row else None


def list_raw_row_candidates(
    raw_row_id: int,
    limit: int = 5,
    connection: sqlite3.Connection | None = None,
    db_path: str | Path | None = None,
) -> list[dict[str, Any]]:
    normalized_limit = _normalize_limit(limit, 5)
    with _managed_connection(connection, db_path) as conn:
        raw_row = get_raw_yadisk_row(raw_row_id, connection=conn)
        if raw_row is None:
            raise ValueError(f"raw row not found: {raw_row_id}")

        lookups = (
            (
                "exact_shk",
                "exact shk",
                find_case_candidates_by_shk(
                    raw_row.get("shk"),
                    limit=normalized_limit,
                    connection=conn,
                ),
            ),
            (
                "exact_tare_transfer",
                "exact tare_transfer",
                find_case_candidates_by_tare_transfer(
                    raw_row.get("tare_transfer"),
                    limit=normalized_limit,
                    connection=conn,
                ),
            ),
            (
                "exact_item_name",
                "exact item_name",
                find_case_candidates_by_item_name(
                    raw_row.get("item_name"),
                    limit=normalized_limit,
                    connection=conn,
                ),
            ),
        )

        results: list[dict[str, Any]] = []
        seen_case_ids: set[str] = set()
        for reason_code, reason_text, candidates in lookups:
            for candidate in candidates:
                case_id = str(candidate["case_id"])
                if case_id in seen_case_ids:
                    continue
                row = dict(candidate)
                row["reason_code"] = reason_code
                row["reason"] = reason_text
                results.append(row)
                seen_case_ids.add(case_id)
                if len(results) >= normalized_limit:
                    return results
        return results


def manual_link_raw_row(
    raw_row_id: int,
    case_id: str,
    actor_id: str,
    note: str | None = None,
    connection: sqlite3.Connection | None = None,
    db_path: str | Path | None = None,
) -> dict[str, Any]:
    normalized_case_id = normalize_empty_value(case_id)
    normalized_actor_id = normalize_empty_value(actor_id)
    normalized_note = normalize_empty_value(note)
    if not normalized_case_id:
        raise ValueError("case_id is required")
    if not normalized_actor_id:
        raise ValueError("actor_id is required")

    with _managed_connection(connection, db_path) as conn:
        raw_row = get_raw_yadisk_row(raw_row_id, connection=conn)
        if raw_row is None:
            raise ValueError(f"raw row not found: {raw_row_id}")
        if get_case_by_case_id(normalized_case_id, connection=conn) is None:
            raise ValueError(f"case not found: {normalized_case_id}")

        is_noop = (
            normalize_empty_value(raw_row.get("matched_case_id")) == normalized_case_id
            and normalize_empty_value(raw_row.get("match_method")) == "manual"
            and normalize_empty_value(raw_row.get("match_confidence")) == "manual"
            and normalize_empty_value(raw_row.get("review_status"))
            == REVIEW_STATUS_LINKED
            and normalize_empty_value(raw_row.get("review_note")) == normalized_note
            and normalize_empty_value(raw_row.get("reviewed_by")) == normalized_actor_id
            and normalize_empty_value(raw_row.get("manual_linked_at")) is not None
        )
        if is_noop:
            return {"changed": False, "raw_row": raw_row}

        now = utc_now_iso()
        updates = {
            "matched_case_id": normalized_case_id,
            "match_method": "manual",
            "match_confidence": "manual",
            "review_status": REVIEW_STATUS_LINKED,
            "review_note": normalized_note,
            "reviewed_at": now,
            "reviewed_by": normalized_actor_id,
            "manual_linked_at": now,
        }
        _update_raw_row(conn, raw_row_id=int(raw_row_id), updates=updates)
        _insert_review_action(
            conn,
            raw_row_id=int(raw_row_id),
            action="manual_link",
            previous_case_id=raw_row.get("matched_case_id"),
            new_case_id=normalized_case_id,
            actor_id=normalized_actor_id,
            note=normalized_note,
            created_at=now,
        )
        updated_row = get_raw_yadisk_row(raw_row_id, connection=conn)
        return {"changed": True, "raw_row": updated_row}


def manual_unlink_raw_row(
    raw_row_id: int,
    actor_id: str,
    note: str | None = None,
    connection: sqlite3.Connection | None = None,
    db_path: str | Path | None = None,
) -> dict[str, Any]:
    normalized_actor_id = normalize_empty_value(actor_id)
    normalized_note = normalize_empty_value(note)
    if not normalized_actor_id:
        raise ValueError("actor_id is required")

    with _managed_connection(connection, db_path) as conn:
        raw_row = get_raw_yadisk_row(raw_row_id, connection=conn)
        if raw_row is None:
            raise ValueError(f"raw row not found: {raw_row_id}")

        previous_case_id = normalize_empty_value(raw_row.get("matched_case_id"))
        is_already_pending_unlinked = (
            previous_case_id is None
            and normalize_empty_value(raw_row.get("review_status"))
            == REVIEW_STATUS_PENDING
        )
        if is_already_pending_unlinked:
            return {"changed": False, "had_link": False, "raw_row": raw_row}

        now = utc_now_iso()
        updates = {
            "matched_case_id": None,
            "match_method": "manual_unlink",
            "match_confidence": "none",
            "review_status": REVIEW_STATUS_PENDING,
            "review_note": normalized_note,
            "reviewed_at": now,
            "reviewed_by": normalized_actor_id,
            "manual_linked_at": None,
            "linked_at": None,
            "link_decision_reason": "manual unlink by operator",
        }
        _update_raw_row(conn, raw_row_id=int(raw_row_id), updates=updates)
        _insert_review_action(
            conn,
            raw_row_id=int(raw_row_id),
            action="manual_unlink",
            previous_case_id=previous_case_id,
            new_case_id=None,
            actor_id=normalized_actor_id,
            note=normalized_note,
            created_at=now,
        )
        updated_row = get_raw_yadisk_row(raw_row_id, connection=conn)
        return {
            "changed": True,
            "had_link": previous_case_id is not None,
            "raw_row": updated_row,
        }


def ignore_raw_row(
    raw_row_id: int,
    actor_id: str,
    note: str | None = None,
    connection: sqlite3.Connection | None = None,
    db_path: str | Path | None = None,
) -> dict[str, Any]:
    normalized_actor_id = normalize_empty_value(actor_id)
    normalized_note = normalize_empty_value(note)
    if not normalized_actor_id:
        raise ValueError("actor_id is required")

    with _managed_connection(connection, db_path) as conn:
        raw_row = get_raw_yadisk_row(raw_row_id, connection=conn)
        if raw_row is None:
            raise ValueError(f"raw row not found: {raw_row_id}")

        is_noop = (
            normalize_empty_value(raw_row.get("review_status"))
            == REVIEW_STATUS_IGNORED
            and normalize_empty_value(raw_row.get("review_note")) == normalized_note
            and normalize_empty_value(raw_row.get("reviewed_by")) == normalized_actor_id
        )
        if is_noop:
            return {"changed": False, "raw_row": raw_row}

        now = utc_now_iso()
        updates = {
            "review_status": REVIEW_STATUS_IGNORED,
            "review_note": normalized_note,
            "reviewed_at": now,
            "reviewed_by": normalized_actor_id,
        }
        _update_raw_row(conn, raw_row_id=int(raw_row_id), updates=updates)
        _insert_review_action(
            conn,
            raw_row_id=int(raw_row_id),
            action="ignore",
            previous_case_id=raw_row.get("matched_case_id"),
            new_case_id=raw_row.get("matched_case_id"),
            actor_id=normalized_actor_id,
            note=normalized_note,
            created_at=now,
        )
        updated_row = get_raw_yadisk_row(raw_row_id, connection=conn)
        return {"changed": True, "raw_row": updated_row}


def mark_raw_row_pending(
    raw_row_id: int,
    actor_id: str,
    note: str | None = None,
    connection: sqlite3.Connection | None = None,
    db_path: str | Path | None = None,
) -> dict[str, Any]:
    normalized_actor_id = normalize_empty_value(actor_id)
    normalized_note = normalize_empty_value(note)
    if not normalized_actor_id:
        raise ValueError("actor_id is required")

    with _managed_connection(connection, db_path) as conn:
        raw_row = get_raw_yadisk_row(raw_row_id, connection=conn)
        if raw_row is None:
            raise ValueError(f"raw row not found: {raw_row_id}")

        is_noop = (
            normalize_empty_value(raw_row.get("review_status"))
            == REVIEW_STATUS_PENDING
            and normalize_empty_value(raw_row.get("review_note")) == normalized_note
            and normalize_empty_value(raw_row.get("reviewed_by")) == normalized_actor_id
        )
        if is_noop:
            return {"changed": False, "raw_row": raw_row}

        now = utc_now_iso()
        updates = {
            "review_status": REVIEW_STATUS_PENDING,
            "review_note": normalized_note,
            "reviewed_at": now,
            "reviewed_by": normalized_actor_id,
        }
        _update_raw_row(conn, raw_row_id=int(raw_row_id), updates=updates)
        _insert_review_action(
            conn,
            raw_row_id=int(raw_row_id),
            action="mark_pending",
            previous_case_id=raw_row.get("matched_case_id"),
            new_case_id=raw_row.get("matched_case_id"),
            actor_id=normalized_actor_id,
            note=normalized_note,
            created_at=now,
        )
        updated_row = get_raw_yadisk_row(raw_row_id, connection=conn)
        return {"changed": True, "raw_row": updated_row}
