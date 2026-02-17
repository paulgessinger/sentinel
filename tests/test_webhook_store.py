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


def make_check_suite_payload(status: str = "in_progress") -> dict:
    return {
        "action": "requested",
        "installation": {"id": 14},
        "repository": {"id": 104, "full_name": "org/repo"},
        "check_suite": {
            "id": 7001,
            "head_sha": "e" * 40,
            "head_branch": "main",
            "status": status,
            "conclusion": None if status != "completed" else "success",
            "app": {"id": 1, "slug": "github-actions"},
            "created_at": "2026-02-16T10:10:00Z",
            "updated_at": "2026-02-16T10:11:00Z",
        },
    }


def make_workflow_run_payload(status: str = "in_progress") -> dict:
    return {
        "action": "requested",
        "installation": {"id": 15},
        "repository": {"id": 105, "full_name": "org/repo"},
        "workflow_run": {
            "id": 8001,
            "name": "Builds",
            "event": "pull_request",
            "status": status,
            "conclusion": None if status != "completed" else "success",
            "head_sha": "f" * 40,
            "run_number": 31,
            "workflow_id": 123456,
            "check_suite_id": 7001,
            "app": {"id": 1, "slug": "github-actions"},
            "created_at": "2026-02-16T10:20:00Z",
            "updated_at": "2026-02-16T10:21:00Z",
        },
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
    assert "check_suites_current" in tables
    assert "workflow_runs_current" in tables
    assert "commit_status_current" in tables
    assert "pr_heads_current" in tables
    assert "sentinel_check_run_state" in tables


def test_initialize_migrates_sentinel_check_run_state_to_nullable_id(tmp_path):
    db_path = tmp_path / "webhooks.sqlite3"

    with sqlite3.connect(str(db_path)) as conn:
        conn.execute(
            """
            CREATE TABLE sentinel_check_run_state (
                repo_id INTEGER NOT NULL,
                repo_full_name TEXT NULL,
                head_sha TEXT NOT NULL,
                check_name TEXT NOT NULL,
                app_id INTEGER NOT NULL,
                check_run_id INTEGER NOT NULL,
                status TEXT NOT NULL,
                conclusion TEXT NULL,
                started_at TEXT NULL,
                completed_at TEXT NULL,
                output_title TEXT NULL,
                output_summary_hash TEXT NULL,
                output_text_hash TEXT NULL,
                last_eval_at TEXT NOT NULL,
                last_publish_at TEXT NULL,
                last_publish_result TEXT NOT NULL,
                last_publish_error TEXT NULL,
                last_delivery_id TEXT NULL,
                PRIMARY KEY (repo_id, head_sha, check_name)
            )
            """
        )
        conn.execute(
            """
            INSERT INTO sentinel_check_run_state (
                repo_id, repo_full_name, head_sha, check_name, app_id, check_run_id,
                status, conclusion, started_at, completed_at, output_title,
                output_summary_hash, output_text_hash, last_eval_at, last_publish_at,
                last_publish_result, last_publish_error, last_delivery_id
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                1,
                "org/repo",
                "a" * 40,
                "merge-sentinel",
                1234,
                -123,
                "in_progress",
                None,
                None,
                None,
                "waiting",
                "h1",
                "h2",
                "2026-02-17T00:00:00.000000Z",
                None,
                "dry_run",
                None,
                "d1",
            ),
        )
        conn.commit()

    store = WebhookStore(str(db_path))
    store.initialize()

    with sqlite3.connect(str(db_path)) as conn:
        notnull = conn.execute(
            """
            SELECT "notnull"
            FROM pragma_table_info('sentinel_check_run_state')
            WHERE name = 'check_run_id'
            """
        ).fetchone()[0]
        check_run_id = conn.execute(
            """
            SELECT check_run_id
            FROM sentinel_check_run_state
            WHERE repo_id = ? AND head_sha = ? AND check_name = ?
            """,
            (1, "a" * 40, "merge-sentinel"),
        ).fetchone()[0]

    assert notnull == 0
    assert check_run_id is None


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


def test_payload_json_is_stored_compressed(tmp_path):
    db_path = tmp_path / "webhooks.sqlite3"
    store = WebhookStore(str(db_path))
    store.initialize()
    payload = make_check_run_payload()
    payload_json = store.payload_to_json(payload)

    store.persist_event(
        delivery_id="compressed-1",
        event="check_run",
        payload=payload,
        payload_json=payload_json,
    )

    with sqlite3.connect(str(db_path)) as conn:
        row = conn.execute(
            "SELECT payload_json FROM webhook_events WHERE delivery_id = ?",
            ("compressed-1",),
        ).fetchone()

    assert isinstance(row[0], bytes)
    assert store.decode_payload_json(row[0]) == payload_json


def test_migrate_payload_json_to_zstd_converts_legacy_plaintext_rows(tmp_path):
    db_path = tmp_path / "webhooks.sqlite3"
    store = WebhookStore(str(db_path))
    store.initialize()

    modern_payload = make_check_run_payload()
    modern_payload_json = store.payload_to_json(modern_payload)
    store.persist_event(
        delivery_id="modern-1",
        event="check_run",
        payload=modern_payload,
        payload_json=modern_payload_json,
    )

    legacy_payload_json = '{"legacy":true}'
    now = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%fZ")
    with sqlite3.connect(str(db_path)) as conn:
        conn.execute(
            """
            INSERT INTO webhook_events (
                delivery_id, received_at, event, payload_json
            ) VALUES (?, ?, ?, ?)
            """,
            ("legacy-1", now, "status", legacy_payload_json),
        )
        conn.commit()

    result = store.migrate_event_payloads_to_zstd(batch_size=1)

    assert result["scanned_rows"] == 2
    assert result["converted_rows"] == 1
    assert result["already_compressed_rows"] == 1
    assert result["skipped_rows"] == 0

    with sqlite3.connect(str(db_path)) as conn:
        migrated = conn.execute(
            "SELECT payload_json FROM webhook_events WHERE delivery_id = ?",
            ("legacy-1",),
        ).fetchone()[0]
    assert isinstance(migrated, bytes)
    assert store.decode_payload_json(migrated) == legacy_payload_json


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
            SELECT repo_full_name, conclusion, last_delivery_id
            FROM check_runs_current
            WHERE repo_id = ? AND check_run_id = ?
            """,
            (101, 2001),
        ).fetchone()

    assert row == ("org/repo", "success", "cr-2")


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
            SELECT repo_full_name, state, last_delivery_id
            FROM commit_status_current
            WHERE repo_id = ? AND sha = ? AND context = ?
            """,
            (102, "b" * 40, "lint"),
        ).fetchone()

    assert row == ("org/repo", "success", "status-2")


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
            SELECT repo_full_name, head_sha, last_delivery_id
            FROM pr_heads_current
            WHERE repo_id = ? AND pr_number = ?
            """,
            (103, 42),
        ).fetchone()

    assert row == ("org/repo", "d" * 40, "pr-2")


def test_check_suite_projection_upserts(tmp_path):
    db_path = tmp_path / "webhooks.sqlite3"
    store = WebhookStore(str(db_path))
    store.initialize()

    store.persist_event(
        delivery_id="suite-1",
        event="check_suite",
        payload=make_check_suite_payload(status="in_progress"),
        payload_json="{}",
    )
    store.persist_event(
        delivery_id="suite-2",
        event="check_suite",
        payload=make_check_suite_payload(status="completed"),
        payload_json="{}",
    )

    with sqlite3.connect(str(db_path)) as conn:
        row = conn.execute(
            """
            SELECT repo_full_name, status, conclusion, last_delivery_id
            FROM check_suites_current
            WHERE repo_id = ? AND check_suite_id = ?
            """,
            (104, 7001),
        ).fetchone()

    assert row == ("org/repo", "completed", "success", "suite-2")


def test_workflow_run_projection_upserts(tmp_path):
    db_path = tmp_path / "webhooks.sqlite3"
    store = WebhookStore(str(db_path))
    store.initialize()

    store.persist_event(
        delivery_id="wfr-1",
        event="workflow_run",
        payload=make_workflow_run_payload(status="in_progress"),
        payload_json="{}",
    )
    store.persist_event(
        delivery_id="wfr-2",
        event="workflow_run",
        payload=make_workflow_run_payload(status="completed"),
        payload_json="{}",
    )

    with sqlite3.connect(str(db_path)) as conn:
        row = conn.execute(
            """
            SELECT repo_full_name, name, status, conclusion, check_suite_id, last_delivery_id
            FROM workflow_runs_current
            WHERE repo_id = ? AND workflow_run_id = ?
            """,
            (105, 8001),
        ).fetchone()

    assert row == ("org/repo", "Builds", "completed", "success", 7001, "wfr-2")


def test_retention_prunes_old_events_only(tmp_path):
    db_path = tmp_path / "webhooks.sqlite3"
    retention_seconds = 60 * 60
    store = WebhookStore(str(db_path), retention_seconds=retention_seconds)
    store.initialize()

    store.persist_event(
        delivery_id="keep-me",
        event="check_run",
        payload=make_check_run_payload(),
        payload_json="{}",
    )

    old_received_at = (
        datetime.now(timezone.utc) - timedelta(seconds=retention_seconds * 2)
    ).strftime("%Y-%m-%dT%H:%M:%S.%fZ")

    with sqlite3.connect(str(db_path)) as conn:
        conn.execute(
            """
            INSERT INTO webhook_events (
                delivery_id, received_at, event, payload_json
            ) VALUES (?, ?, ?, ?)
            """,
            (
                "old-event",
                old_received_at,
                "status",
                store.encode_payload_json("{}"),
            ),
        )
        conn.commit()

    pruned = store.prune_old_events()
    assert pruned == 1

    with sqlite3.connect(str(db_path)) as conn:
        remaining = conn.execute("SELECT COUNT(*) FROM webhook_events").fetchone()[0]
        projected = conn.execute("SELECT COUNT(*) FROM check_runs_current").fetchone()[0]

    assert remaining == 1
    assert projected == 1


def test_prune_old_projections_uses_completed_and_active_windows(tmp_path):
    db_path = tmp_path / "webhooks.sqlite3"
    store = WebhookStore(str(db_path))
    store.initialize()

    now = datetime.now(timezone.utc)
    completed_old = (now - timedelta(seconds=4000)).strftime("%Y-%m-%dT%H:%M:%S.%fZ")
    active_old = (now - timedelta(seconds=8000)).strftime("%Y-%m-%dT%H:%M:%S.%fZ")
    active_recent = (now - timedelta(seconds=3000)).strftime("%Y-%m-%dT%H:%M:%S.%fZ")

    with sqlite3.connect(str(db_path)) as conn:
        conn.execute(
            """
            INSERT INTO check_runs_current (
                repo_id, repo_full_name, check_run_id, head_sha, name, status, conclusion,
                app_id, app_slug, check_suite_id, started_at, completed_at, first_seen_at,
                last_seen_at, last_delivery_id
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                1,
                "org/repo",
                1,
                "a" * 40,
                "build",
                "completed",
                "success",
                None,
                None,
                None,
                None,
                None,
                completed_old,
                completed_old,
                "d1",
            ),
        )
        conn.execute(
            """
            INSERT INTO check_runs_current (
                repo_id, repo_full_name, check_run_id, head_sha, name, status, conclusion,
                app_id, app_slug, check_suite_id, started_at, completed_at, first_seen_at,
                last_seen_at, last_delivery_id
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                1,
                "org/repo",
                2,
                "b" * 40,
                "tests",
                "in_progress",
                None,
                None,
                None,
                None,
                None,
                None,
                active_recent,
                active_recent,
                "d2",
            ),
        )
        conn.execute(
            """
            INSERT INTO check_runs_current (
                repo_id, repo_full_name, check_run_id, head_sha, name, status, conclusion,
                app_id, app_slug, check_suite_id, started_at, completed_at, first_seen_at,
                last_seen_at, last_delivery_id
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                1,
                "org/repo",
                3,
                "c" * 40,
                "lint",
                "queued",
                None,
                None,
                None,
                None,
                None,
                None,
                active_old,
                active_old,
                "d3",
            ),
        )
        conn.execute(
            """
            INSERT INTO commit_status_current (
                repo_id, repo_full_name, sha, context, status_id, state, created_at,
                updated_at, url, first_seen_at, last_seen_at, last_delivery_id
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                1,
                "org/repo",
                "d" * 40,
                "status-success",
                10,
                "success",
                None,
                None,
                None,
                completed_old,
                completed_old,
                "d4",
            ),
        )
        conn.execute(
            """
            INSERT INTO commit_status_current (
                repo_id, repo_full_name, sha, context, status_id, state, created_at,
                updated_at, url, first_seen_at, last_seen_at, last_delivery_id
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                1,
                "org/repo",
                "e" * 40,
                "status-pending",
                11,
                "pending",
                None,
                None,
                None,
                active_recent,
                active_recent,
                "d5",
            ),
        )
        conn.execute(
            """
            INSERT INTO pr_heads_current (
                repo_id, repo_full_name, pr_id, pr_number, state, head_sha, base_ref,
                action, updated_at, last_delivery_id
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                1,
                "org/repo",
                100,
                1,
                "closed",
                "f" * 40,
                "main",
                "closed",
                completed_old,
                "d6",
            ),
        )
        conn.execute(
            """
            INSERT INTO pr_heads_current (
                repo_id, repo_full_name, pr_id, pr_number, state, head_sha, base_ref,
                action, updated_at, last_delivery_id
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                1,
                "org/repo",
                101,
                2,
                "open",
                "g" * 40,
                "main",
                "synchronize",
                active_recent,
                "d7",
            ),
        )
        conn.commit()

    counts = store.prune_old_projections(
        completed_retention_seconds=3600,
        active_retention_seconds=7200,
    )

    assert counts["check_runs_current:completed"] == 1
    assert counts["check_runs_current:active"] == 1
    assert counts["commit_status_current:completed"] == 1
    assert counts["commit_status_current:active"] == 0
    assert counts["pr_heads_current:completed"] == 1
    assert counts["pr_heads_current:active"] == 0

    with sqlite3.connect(str(db_path)) as conn:
        run_count = conn.execute("SELECT COUNT(*) FROM check_runs_current").fetchone()[0]
        status_count = conn.execute(
            "SELECT COUNT(*) FROM commit_status_current"
        ).fetchone()[0]
        pr_count = conn.execute("SELECT COUNT(*) FROM pr_heads_current").fetchone()[0]

    assert run_count == 1
    assert status_count == 1
    assert pr_count == 1
