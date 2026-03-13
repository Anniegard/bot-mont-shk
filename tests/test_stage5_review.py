from __future__ import annotations

from bot.config import parse_admin_user_ids
from bot.db import insert_raw_yadisk_row
from bot.services.raw_review import (
    ignore_raw_row,
    manual_link_raw_row,
    manual_unlink_raw_row,
    mark_raw_row_pending,
)


def _insert_raw_row(connection, **overrides: object) -> int:
    payload = {
        "row_hash": f"raw-hash-{overrides.get('row_hash_suffix', '1')}",
        "source_file_name": "report.xlsx",
        "source_path": "telegram:file-1",
        "source_kind": "no_move",
        "source_sheet_name": "Sheet1",
        "source_row_number": 2,
        "shk": "SHK-1",
        "tare_transfer": "TARE-1",
        "item_name": "Item 1",
        "normalized_json": {"raw": "value"},
    }
    payload.update(overrides)
    payload.pop("row_hash_suffix", None)
    return insert_raw_yadisk_row(connection=connection, **payload)


def test_manual_link_raw_row_sets_manual_review_and_audit(connection, seed_case) -> None:
    seed_case("case-1", shk="SHK-1", item_name="Item 1")
    raw_row_id = _insert_raw_row(connection, row_hash_suffix="manual-link")

    result = manual_link_raw_row(
        raw_row_id=raw_row_id,
        case_id="case-1",
        actor_id="1001",
        note="manual link",
        connection=connection,
    )

    assert result["changed"] is True
    row = connection.execute(
        "SELECT * FROM raw_yadisk_rows WHERE id = ?",
        (raw_row_id,),
    ).fetchone()
    assert row["matched_case_id"] == "case-1"
    assert row["match_method"] == "manual"
    assert row["match_confidence"] == "manual"
    assert row["review_status"] == "linked"
    assert row["review_note"] == "manual link"
    assert row["reviewed_by"] == "1001"
    assert row["manual_linked_at"] is not None

    audit_row = connection.execute(
        "SELECT * FROM raw_review_actions WHERE raw_row_id = ?",
        (raw_row_id,),
    ).fetchone()
    assert audit_row["action"] == "manual_link"
    assert audit_row["previous_case_id"] is None
    assert audit_row["new_case_id"] == "case-1"
    assert audit_row["actor_id"] == "1001"
    assert audit_row["note"] == "manual link"


def test_ignore_raw_row_marks_row_ignored_and_preserves_record(connection) -> None:
    raw_row_id = _insert_raw_row(connection, row_hash_suffix="ignore")

    result = ignore_raw_row(
        raw_row_id=raw_row_id,
        actor_id="1002",
        note="ignore row",
        connection=connection,
    )

    assert result["changed"] is True
    row = connection.execute(
        "SELECT * FROM raw_yadisk_rows WHERE id = ?",
        (raw_row_id,),
    ).fetchone()
    assert row["review_status"] == "ignored"
    assert row["review_note"] == "ignore row"
    assert row["reviewed_by"] == "1002"

    row_count = connection.execute(
        "SELECT COUNT(*) AS count FROM raw_yadisk_rows WHERE id = ?",
        (raw_row_id,),
    ).fetchone()
    assert int(row_count["count"]) == 1

    audit_row = connection.execute(
        "SELECT * FROM raw_review_actions WHERE raw_row_id = ?",
        (raw_row_id,),
    ).fetchone()
    assert audit_row["action"] == "ignore"
    assert audit_row["actor_id"] == "1002"
    assert audit_row["note"] == "ignore row"


def test_manual_unlink_raw_row_clears_link_and_writes_audit(
    connection, seed_case
) -> None:
    seed_case("case-1", shk="SHK-1", item_name="Item 1")
    raw_row_id = _insert_raw_row(connection, row_hash_suffix="unlink")
    manual_link_raw_row(
        raw_row_id=raw_row_id,
        case_id="case-1",
        actor_id="1001",
        note="manual link",
        connection=connection,
    )

    result = manual_unlink_raw_row(
        raw_row_id=raw_row_id,
        actor_id="1004",
        note="manual unlink",
        connection=connection,
    )

    assert result["changed"] is True
    assert result["had_link"] is True
    row = connection.execute(
        "SELECT * FROM raw_yadisk_rows WHERE id = ?",
        (raw_row_id,),
    ).fetchone()
    assert row["matched_case_id"] is None
    assert row["match_method"] == "manual_unlink"
    assert row["match_confidence"] == "none"
    assert row["review_status"] == "pending"
    assert row["review_note"] == "manual unlink"
    assert row["reviewed_by"] == "1004"
    assert row["manual_linked_at"] is None
    assert row["linked_at"] is None
    assert row["link_decision_reason"] == "manual unlink by operator"

    audit_row = connection.execute(
        """
        SELECT *
        FROM raw_review_actions
        WHERE raw_row_id = ?
        ORDER BY id DESC
        LIMIT 1
        """,
        (raw_row_id,),
    ).fetchone()
    assert audit_row["action"] == "manual_unlink"
    assert audit_row["previous_case_id"] == "case-1"
    assert audit_row["new_case_id"] is None
    assert audit_row["actor_id"] == "1004"
    assert audit_row["note"] == "manual unlink"


def test_mark_raw_row_pending_moves_ignored_row_back_to_pending(connection) -> None:
    raw_row_id = _insert_raw_row(connection, row_hash_suffix="pending")
    ignore_raw_row(
        raw_row_id=raw_row_id,
        actor_id="1002",
        note="temporary ignore",
        connection=connection,
    )

    result = mark_raw_row_pending(
        raw_row_id=raw_row_id,
        actor_id="1003",
        note="reopen review",
        connection=connection,
    )

    assert result["changed"] is True
    row = connection.execute(
        "SELECT * FROM raw_yadisk_rows WHERE id = ?",
        (raw_row_id,),
    ).fetchone()
    assert row["review_status"] == "pending"
    assert row["matched_case_id"] is None
    assert row["review_note"] == "reopen review"
    assert row["reviewed_by"] == "1003"

    audit_rows = connection.execute(
        """
        SELECT action, previous_case_id, new_case_id
        FROM raw_review_actions
        WHERE raw_row_id = ?
        ORDER BY id ASC
        """,
        (raw_row_id,),
    ).fetchall()
    assert [row["action"] for row in audit_rows] == ["ignore", "mark_pending"]
    assert audit_rows[-1]["previous_case_id"] is None
    assert audit_rows[-1]["new_case_id"] is None


def test_mark_raw_row_pending_preserves_existing_link(connection, seed_case) -> None:
    seed_case("case-1", shk="SHK-1", item_name="Item 1")
    raw_row_id = _insert_raw_row(connection, row_hash_suffix="pending-linked")
    manual_link_raw_row(
        raw_row_id=raw_row_id,
        case_id="case-1",
        actor_id="1001",
        note="manual link",
        connection=connection,
    )

    linked_row = connection.execute(
        "SELECT * FROM raw_yadisk_rows WHERE id = ?",
        (raw_row_id,),
    ).fetchone()
    manual_linked_at = linked_row["manual_linked_at"]

    result = mark_raw_row_pending(
        raw_row_id=raw_row_id,
        actor_id="1005",
        note="reopen linked row",
        connection=connection,
    )

    assert result["changed"] is True
    row = connection.execute(
        "SELECT * FROM raw_yadisk_rows WHERE id = ?",
        (raw_row_id,),
    ).fetchone()
    assert row["matched_case_id"] == "case-1"
    assert row["match_method"] == "manual"
    assert row["match_confidence"] == "manual"
    assert row["review_status"] == "pending"
    assert row["review_note"] == "reopen linked row"
    assert row["reviewed_by"] == "1005"
    assert row["manual_linked_at"] == manual_linked_at

    audit_row = connection.execute(
        """
        SELECT *
        FROM raw_review_actions
        WHERE raw_row_id = ?
        ORDER BY id DESC
        LIMIT 1
        """,
        (raw_row_id,),
    ).fetchone()
    assert audit_row["action"] == "mark_pending"
    assert audit_row["previous_case_id"] == "case-1"
    assert audit_row["new_case_id"] == "case-1"


def test_parse_admin_user_ids_supports_bot_admin_ids_and_legacy_id() -> None:
    parsed = parse_admin_user_ids(
        bot_admin_ids="1001, 1002,1001",
        legacy_admin_user_id="1003",
    )

    assert parsed == ("1001", "1002", "1003")
