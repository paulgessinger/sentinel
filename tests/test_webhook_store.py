from datetime import datetime, timedelta, timezone
import sqlite3

from sentinel.storage import WebhookStore


def make_check_run_payload(conclusion: str = "success") -> dict:
    return {
        "action": "completed",
        "installation": {"id": 11},
        "repository": {"id": 101, "full_name": "org/repo"},
        "check_run": {
            "id": 2001,
            "head_sha": "a" * 40,
            "name": "tests",
            "status": "completed",
            "conclusion": conclusion,
            "started_at": "2026-02-16T10:00:00Z",
            "completed_at": "2026-02-16T10:03:00Z",
            "app": {"id": 1, "slug": "ci"},
            "check_suite": {"id": 77},
        },
    }


def make_status_payload(state: str = "pending") -> dict:
    return {
        "id": 9001,
        "sha": "b" * 40,
        "context": "lint",
        "state": state,
        "url": "https://api.github.com/repos/org/repo/statuses/1",
        "created_at": "2026-02-16T09:00:00Z",
        "updated_at": "2026-02-16T09:01:00Z",
        "installation": {"id": 12},
        "repository": {"id": 102, "full_name": "org/repo"},
    }


def make_pull_request_payload(head_sha: str) -> dict:
    return {
        "action": "synchronize",
        "installation": {"id": 13},
        "repository": {"id": 103, "full_name": "org/repo"},
        "pull_request": {
            "id": 5001,
            "number": 42,
            "state": "open",
            "updated_at": "2026-02-16T09:30:00Z",
            "head": {"sha": head_sha},
            "base": {"ref": "main"},
        },
    }


def test_initialize_schema(tmp_path):
    db_path = tmp_path / "webhooks.sqlite3"
    store = WebhookStore(str(db_path))
    store.initialize()

    with sqlite3.connect(str(db_path)) as conn:
        tables = {
            row[0]
            for row in conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table'"
            ).fetchall()
        }

    assert "webhook_events" in tables
    assert "check_runs_current" in tables
    assert "commit_status_current" in tables
    assert "pr_heads_current" in tables


def test_duplicate_delivery_id_is_ignored(tmp_path):
    db_path = tmp_path / "webhooks.sqlite3"
    store = WebhookStore(str(db_path))
    store.initialize()
    payload = make_check_run_payload()
    payload_json = store.payload_to_json(payload)

    first = store.persist_event(
        delivery_id="delivery-1",
        event="check_run",
        payload=payload,
        payload_json=payload_json,
    )
    second = store.persist_event(
        delivery_id="delivery-1",
        event="check_run",
        payload=payload,
        payload_json=payload_json,
    )

    assert first.inserted
    assert second.duplicate

    with sqlite3.connect(str(db_path)) as conn:
        count = conn.execute("SELECT COUNT(*) FROM webhook_events").fetchone()[0]
    assert count == 1


def test_check_run_projection_upserts(tmp_path):
    db_path = tmp_path / "webhooks.sqlite3"
    store = WebhookStore(str(db_path))
    store.initialize()

    store.persist_event(
        delivery_id="cr-1",
        event="check_run",
        payload=make_check_run_payload(conclusion="failure"),
        payload_json="{}",
    )
    store.persist_event(
        delivery_id="cr-2",
        event="check_run",
        payload=make_check_run_payload(conclusion="success"),
        payload_json="{}",
    )

    with sqlite3.connect(str(db_path)) as conn:
        row = conn.execute(
            """
            SELECT conclusion, last_delivery_id
            FROM check_runs_current
            WHERE repo_id = ? AND check_run_id = ?
            """,
            (101, 2001),
        ).fetchone()

    assert row == ("success", "cr-2")


def test_status_projection_updates_state(tmp_path):
    db_path = tmp_path / "webhooks.sqlite3"
    store = WebhookStore(str(db_path))
    store.initialize()

    store.persist_event(
        delivery_id="status-1",
        event="status",
        payload=make_status_payload(state="pending"),
        payload_json="{}",
    )
    store.persist_event(
        delivery_id="status-2",
        event="status",
        payload=make_status_payload(state="success"),
        payload_json="{}",
    )

    with sqlite3.connect(str(db_path)) as conn:
        row = conn.execute(
            """
            SELECT state, last_delivery_id
            FROM commit_status_current
            WHERE repo_id = ? AND sha = ? AND context = ?
            """,
            (102, "b" * 40, "lint"),
        ).fetchone()

    assert row == ("success", "status-2")


def test_pull_request_projection_updates_head_sha(tmp_path):
    db_path = tmp_path / "webhooks.sqlite3"
    store = WebhookStore(str(db_path))
    store.initialize()

    store.persist_event(
        delivery_id="pr-1",
        event="pull_request",
        payload=make_pull_request_payload(head_sha="c" * 40),
        payload_json="{}",
    )
    store.persist_event(
        delivery_id="pr-2",
        event="pull_request",
        payload=make_pull_request_payload(head_sha="d" * 40),
        payload_json="{}",
    )

    with sqlite3.connect(str(db_path)) as conn:
        row = conn.execute(
            """
            SELECT head_sha, last_delivery_id
            FROM pr_heads_current
            WHERE repo_id = ? AND pr_number = ?
            """,
            (103, 42),
        ).fetchone()

    assert row == ("d" * 40, "pr-2")


def test_retention_prunes_old_events_only(tmp_path):
    db_path = tmp_path / "webhooks.sqlite3"
    store = WebhookStore(str(db_path), retention_days=30)
    store.initialize()

    store.persist_event(
        delivery_id="keep-me",
        event="check_run",
        payload=make_check_run_payload(),
        payload_json="{}",
    )

    old_received_at = (
        datetime.now(timezone.utc) - timedelta(days=45)
    ).strftime("%Y-%m-%dT%H:%M:%S.%fZ")

    with sqlite3.connect(str(db_path)) as conn:
        conn.execute(
            """
            INSERT INTO webhook_events (
                delivery_id, received_at, event, payload_json
            ) VALUES (?, ?, ?, ?)
            """,
            ("old-event", old_received_at, "status", "{}"),
        )
        conn.commit()

    pruned = store.prune_old_events()
    assert pruned == 1

    with sqlite3.connect(str(db_path)) as conn:
        remaining = conn.execute("SELECT COUNT(*) FROM webhook_events").fetchone()[0]
        projected = conn.execute("SELECT COUNT(*) FROM check_runs_current").fetchone()[0]

    assert remaining == 1
    assert projected == 1
