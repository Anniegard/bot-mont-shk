from __future__ import annotations

import sqlite3
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Iterator

from bot.db import (
    get_case_by_case_id as db_get_case_by_case_id,
    get_db_connection,
    normalize_empty_value,
)

DEFAULT_CASE_LIMIT = 10
DEFAULT_CASE_RAW_LIMIT = 20
DEFAULT_RAW_LIMIT = 10

_CASE_ORDER_SQL = "c.updated_at DESC, c.case_id ASC"
_RAW_ORDER_SQL = "r.imported_at DESC, r.id DESC"


@contextmanager
def _managed_connection(
    connection: sqlite3.Connection | None = None,
    db_path: str | Path | None = None,
) -> Iterator[sqlite3.Connection]:
    owns_connection = connection is None
    conn = connection or get_db_connection(db_path)
    try:
        yield conn
    finally:
        if owns_connection:
            conn.close()


def _normalize_limit(limit: int | None, default: int) -> int:
    try:
        return max(1, int(limit or default))
    except (TypeError, ValueError):
        return default


def _normalize_search_text(value: str | None, *, lowercase: bool = True) -> str | None:
    normalized = normalize_empty_value(value)
    if normalized is None:
        return None
    text = " ".join(str(normalized).replace("\xa0", " ").split())
    if not text:
        return None
    return text.lower() if lowercase else text


def _normalized_sql(column_name: str) -> str:
    return (
        "lower(trim("
        "replace("
        "replace("
        "replace("
        "replace(ifnull("
        f"{column_name}"
        ", ''), char(160), ' '), "
        "'  ', ' '), "
        "'  ', ' '), "
        "'  ', ' ')"
        "))"
    )


def _escape_like(value: str) -> str:
    return value.replace("\\", "\\\\").replace("%", "\\%").replace("_", "\\_")


def _annotate_rows(
    rows: list[dict[str, Any]],
    *,
    match_field: str,
    match_type: str,
) -> list[dict[str, Any]]:
    for row in rows:
        row["match_field"] = match_field
        row["match_type"] = match_type
    return rows


def _fetch_case_rows(
    conn: sqlite3.Connection,
    *,
    match_field: str,
    match_type: str,
    case_expression: str,
    item_expression: str,
    parameters: tuple[Any, ...],
    limit: int,
) -> list[dict[str, Any]]:
    rows = conn.execute(
        f"""
        SELECT c.*
        FROM cases AS c
        WHERE c.is_active = 1
          AND (
            {case_expression}
            OR EXISTS (
                SELECT 1
                FROM case_items AS ci
                WHERE ci.case_id = c.case_id
                  AND {item_expression}
            )
          )
        ORDER BY {_CASE_ORDER_SQL}
        LIMIT ?
        """,
        (*parameters, *parameters, limit),
    ).fetchall()
    return _annotate_rows(
        [dict(row) for row in rows],
        match_field=match_field,
        match_type=match_type,
    )


def _fetch_raw_rows(
    conn: sqlite3.Connection,
    *,
    match_field: str,
    match_type: str,
    expression: str,
    parameters: tuple[Any, ...],
    limit: int,
) -> list[dict[str, Any]]:
    rows = conn.execute(
        f"""
        SELECT
            r.id,
            r.source_kind,
            r.source_sheet_name,
            r.source_row_number,
            r.source_file_name,
            r.source_path,
            r.shk,
            r.tare_transfer,
            r.item_name,
            r.matched_case_id,
            r.match_method,
            r.match_confidence,
            r.review_status,
            r.imported_at
        FROM raw_yadisk_rows AS r
        WHERE {expression}
        ORDER BY {_RAW_ORDER_SQL}
        LIMIT ?
        """,
        (*parameters, limit),
    ).fetchall()
    return _annotate_rows(
        [dict(row) for row in rows],
        match_field=match_field,
        match_type=match_type,
    )


def _find_cases_by_text_field(
    *,
    match_field: str,
    value: str | None,
    case_column: str,
    item_column: str,
    limit: int,
    allow_partial: bool = False,
    connection: sqlite3.Connection | None = None,
    db_path: str | Path | None = None,
) -> list[dict[str, Any]]:
    strict_query = _normalize_search_text(value, lowercase=False)
    if strict_query is None:
        return []

    normalized_query = _normalize_search_text(strict_query)
    assert normalized_query is not None

    normalized_case_column = _normalized_sql(f"c.{case_column}")
    normalized_item_column = _normalized_sql(f"ci.{item_column}")
    normalized_limit = _normalize_limit(limit, DEFAULT_CASE_LIMIT)

    with _managed_connection(connection, db_path) as conn:
        exact_rows = _fetch_case_rows(
            conn,
            match_field=match_field,
            match_type="exact",
            case_expression=f"trim(c.{case_column}) = ?",
            item_expression=f"trim(ci.{item_column}) = ?",
            parameters=(strict_query,),
            limit=normalized_limit,
        )
        if exact_rows:
            return exact_rows

        normalized_rows = _fetch_case_rows(
            conn,
            match_field=match_field,
            match_type="normalized_exact",
            case_expression=f"{normalized_case_column} = ?",
            item_expression=f"{normalized_item_column} = ?",
            parameters=(normalized_query,),
            limit=normalized_limit,
        )
        if normalized_rows or not allow_partial:
            return normalized_rows

        like_value = f"%{_escape_like(normalized_query)}%"
        return _fetch_case_rows(
            conn,
            match_field=match_field,
            match_type="partial",
            case_expression=f"{normalized_case_column} LIKE ? ESCAPE '\\'",
            item_expression=f"{normalized_item_column} LIKE ? ESCAPE '\\'",
            parameters=(like_value,),
            limit=normalized_limit,
        )


def _find_raw_rows_by_text_field(
    *,
    match_field: str,
    value: str | None,
    column: str,
    limit: int,
    allow_partial: bool = False,
    connection: sqlite3.Connection | None = None,
    db_path: str | Path | None = None,
) -> list[dict[str, Any]]:
    strict_query = _normalize_search_text(value, lowercase=False)
    if strict_query is None:
        return []

    normalized_query = _normalize_search_text(strict_query)
    assert normalized_query is not None

    normalized_limit = _normalize_limit(limit, DEFAULT_RAW_LIMIT)
    normalized_column = _normalized_sql(f"r.{column}")

    with _managed_connection(connection, db_path) as conn:
        exact_rows = _fetch_raw_rows(
            conn,
            match_field=match_field,
            match_type="exact",
            expression=f"trim(r.{column}) = ?",
            parameters=(strict_query,),
            limit=normalized_limit,
        )
        if exact_rows:
            return exact_rows

        normalized_rows = _fetch_raw_rows(
            conn,
            match_field=match_field,
            match_type="normalized_exact",
            expression=f"{normalized_column} = ?",
            parameters=(normalized_query,),
            limit=normalized_limit,
        )
        if normalized_rows or not allow_partial:
            return normalized_rows

        like_value = f"%{_escape_like(normalized_query)}%"
        return _fetch_raw_rows(
            conn,
            match_field=match_field,
            match_type="partial",
            expression=f"{normalized_column} LIKE ? ESCAPE '\\'",
            parameters=(like_value,),
            limit=normalized_limit,
        )


def get_case_by_case_id(
    case_id: str,
    connection: sqlite3.Connection | None = None,
    db_path: str | Path | None = None,
) -> dict[str, Any] | None:
    case_row = db_get_case_by_case_id(case_id, connection=connection, db_path=db_path)
    if case_row is None:
        return None
    case_row["match_field"] = "case_id"
    case_row["match_type"] = "exact"
    return case_row


def find_cases_by_shk(
    shk: str,
    limit: int = DEFAULT_CASE_LIMIT,
    connection: sqlite3.Connection | None = None,
    db_path: str | Path | None = None,
) -> list[dict[str, Any]]:
    return _find_cases_by_text_field(
        match_field="shk",
        value=shk,
        case_column="shk",
        item_column="shk",
        limit=limit,
        connection=connection,
        db_path=db_path,
    )


def find_cases_by_tare_transfer(
    tare_transfer: str,
    limit: int = DEFAULT_CASE_LIMIT,
    connection: sqlite3.Connection | None = None,
    db_path: str | Path | None = None,
) -> list[dict[str, Any]]:
    return _find_cases_by_text_field(
        match_field="tare_transfer",
        value=tare_transfer,
        case_column="tare_transfer",
        item_column="tare_transfer",
        limit=limit,
        connection=connection,
        db_path=db_path,
    )


def find_cases_by_item_name(
    item_name: str,
    limit: int = DEFAULT_CASE_LIMIT,
    connection: sqlite3.Connection | None = None,
    db_path: str | Path | None = None,
) -> list[dict[str, Any]]:
    return _find_cases_by_text_field(
        match_field="item_name",
        value=item_name,
        case_column="item_name",
        item_column="item_name",
        limit=limit,
        allow_partial=True,
        connection=connection,
        db_path=db_path,
    )


def search_cases(
    query: str,
    limit: int = DEFAULT_CASE_LIMIT,
    connection: sqlite3.Connection | None = None,
    db_path: str | Path | None = None,
) -> dict[str, Any]:
    normalized_limit = _normalize_limit(limit, DEFAULT_CASE_LIMIT)
    exact_case = get_case_by_case_id(query, connection=connection, db_path=db_path)
    if exact_case is not None:
        return {
            "query": _normalize_search_text(query, lowercase=False),
            "match_field": "case_id",
            "match_type": "exact",
            "results": [exact_case],
            "truncated": False,
        }

    for finder in (
        find_cases_by_shk,
        find_cases_by_tare_transfer,
        find_cases_by_item_name,
    ):
        rows = finder(
            query,
            limit=normalized_limit + 1,
            connection=connection,
            db_path=db_path,
        )
        if rows:
            return {
                "query": _normalize_search_text(query, lowercase=False),
                "match_field": rows[0]["match_field"],
                "match_type": rows[0]["match_type"],
                "results": rows[:normalized_limit],
                "truncated": len(rows) > normalized_limit,
            }

    return {
        "query": _normalize_search_text(query, lowercase=False),
        "match_field": None,
        "match_type": None,
        "results": [],
        "truncated": False,
    }


def find_raw_rows_by_shk(
    shk: str,
    limit: int = DEFAULT_RAW_LIMIT,
    connection: sqlite3.Connection | None = None,
    db_path: str | Path | None = None,
) -> list[dict[str, Any]]:
    return _find_raw_rows_by_text_field(
        match_field="shk",
        value=shk,
        column="shk",
        limit=limit,
        connection=connection,
        db_path=db_path,
    )


def find_raw_rows_by_tare_transfer(
    tare_transfer: str,
    limit: int = DEFAULT_RAW_LIMIT,
    connection: sqlite3.Connection | None = None,
    db_path: str | Path | None = None,
) -> list[dict[str, Any]]:
    return _find_raw_rows_by_text_field(
        match_field="tare_transfer",
        value=tare_transfer,
        column="tare_transfer",
        limit=limit,
        connection=connection,
        db_path=db_path,
    )


def find_raw_rows_by_item_name(
    item_name: str,
    limit: int = DEFAULT_RAW_LIMIT,
    connection: sqlite3.Connection | None = None,
    db_path: str | Path | None = None,
) -> list[dict[str, Any]]:
    return _find_raw_rows_by_text_field(
        match_field="item_name",
        value=item_name,
        column="item_name",
        limit=limit,
        allow_partial=True,
        connection=connection,
        db_path=db_path,
    )


def search_raw_rows(
    query: str,
    limit: int = DEFAULT_RAW_LIMIT,
    connection: sqlite3.Connection | None = None,
    db_path: str | Path | None = None,
) -> dict[str, Any]:
    normalized_limit = _normalize_limit(limit, DEFAULT_RAW_LIMIT)
    for finder in (
        find_raw_rows_by_shk,
        find_raw_rows_by_tare_transfer,
        find_raw_rows_by_item_name,
    ):
        rows = finder(
            query,
            limit=normalized_limit + 1,
            connection=connection,
            db_path=db_path,
        )
        if rows:
            return {
                "query": _normalize_search_text(query, lowercase=False),
                "match_field": rows[0]["match_field"],
                "match_type": rows[0]["match_type"],
                "results": rows[:normalized_limit],
                "truncated": len(rows) > normalized_limit,
            }

    return {
        "query": _normalize_search_text(query, lowercase=False),
        "match_field": None,
        "match_type": None,
        "results": [],
        "truncated": False,
    }


def get_raw_rows_for_case(
    case_id: str,
    limit: int = DEFAULT_CASE_RAW_LIMIT,
    connection: sqlite3.Connection | None = None,
    db_path: str | Path | None = None,
) -> list[dict[str, Any]]:
    normalized_case_id = normalize_empty_value(case_id)
    if normalized_case_id is None:
        return []

    normalized_limit = _normalize_limit(limit, DEFAULT_CASE_RAW_LIMIT)
    with _managed_connection(connection, db_path) as conn:
        rows = conn.execute(
            f"""
            SELECT
                r.id,
                r.source_kind,
                r.source_sheet_name,
                r.source_row_number,
                r.source_file_name,
                r.source_path,
                r.shk,
                r.tare_transfer,
                r.item_name,
                r.matched_case_id,
                r.match_method,
                r.match_confidence,
                r.review_status,
                r.imported_at
            FROM raw_yadisk_rows AS r
            WHERE r.matched_case_id = ?
            ORDER BY {_RAW_ORDER_SQL}
            LIMIT ?
            """,
            (normalized_case_id, normalized_limit),
        ).fetchall()
        return [dict(row) for row in rows]
