from __future__ import annotations

from collections.abc import Iterator
from pathlib import Path

import pytest

from bot.db import get_db_connection, init_db, upsert_case


@pytest.fixture
def db_path(tmp_path: Path) -> Path:
    path = tmp_path / "test.db"
    init_db(path)
    return path


@pytest.fixture
def connection(db_path: Path) -> Iterator:
    conn = get_db_connection(db_path)
    try:
        yield conn
    finally:
        conn.close()


@pytest.fixture
def seed_case(connection):
    def _seed_case(case_id: str, **case_fields: object) -> str:
        upsert_case(
            case_id=case_id,
            source_sheet_name="master",
            sheet_row_number=2,
            case_fields=case_fields,
            connection=connection,
        )
        return case_id

    return _seed_case
