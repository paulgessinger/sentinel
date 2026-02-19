from datetime import datetime, timedelta, timezone
from pathlib import Path
import sqlite3

from sentinel.config import SETTINGS
from sentinel.storage import WebhookStore


def _make_store(db_path, **updates):
    return WebhookStore(
        settings=SETTINGS.model_copy(
            update={
                "WEBHOOK_DB_PATH": Path(db_path),
                **updates,
            }
        )
    )


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


def make_pull_request_payload(
    head_sha: str,
    title: str = "Update branch",
    *,
    action: str = "synchronize",
    state: str = "open",
    draft: bool | None = None,
    merged: bool | None = None,
) -> dict:
    pull_request = {
        "id": 5001,
        "number": 42,
        "title": title,
        "state": state,
        "updated_at": "2026-02-16T09:30:00Z",
        "head": {"sha": head_sha},
        "base": {"ref": "main"},
    }
    if draft is not None:
        pull_request["draft"] = draft
    if merged is not None:
        pull_request["merged"] = merged

    return {
        "action": action,
        "installation": {"id": 13},
        "repository": {"id": 103, "full_name": "org/repo"},
        "pull_request": pull_request,
    }


def test_initialize_schema(tmp_path):
    db_path = tmp_path / "webhooks.sqlite3"
    store = _make_store(db_path)
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
    assert "sentinel_activity_events" in tables


def test_duplicate_delivery_id_is_ignored(tmp_path):
    db_path = tmp_path / "webhooks.sqlite3"
    store = _make_store(db_path)
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
    store = _make_store(db_path)
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
    store = _make_store(db_path)
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
    store = _make_store(db_path)
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
    store = _make_store(db_path)
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
    store = _make_store(db_path)
    store.initialize()

    store.persist_event(
        delivery_id="pr-1",
        event="pull_request",
        payload=make_pull_request_payload(
            head_sha="c" * 40, title="Initial title", draft=False
        ),
        payload_json="{}",
    )
    store.persist_event(
        delivery_id="pr-2",
        event="pull_request",
        payload=make_pull_request_payload(
            head_sha="d" * 40, title="Updated title", draft=True
        ),
        payload_json="{}",
    )

    with sqlite3.connect(str(db_path)) as conn:
        row = conn.execute(
            """
            SELECT repo_full_name, pr_title, pr_draft, head_sha, last_delivery_id
            FROM pr_heads_current
            WHERE repo_id = ? AND pr_number = ?
            """,
            (103, 42),
        ).fetchone()

    assert row is not None
    assert row[0] == "org/repo"
    assert row[1] == "Updated title"
    assert bool(row[2]) is True
    assert row[3] == "d" * 40
    assert row[4] == "pr-2"


def test_check_suite_projection_upserts(tmp_path):
    db_path = tmp_path / "webhooks.sqlite3"
    store = _make_store(db_path)
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
    store = _make_store(db_path)
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
    store = _make_store(db_path, WEBHOOK_DB_RETENTION_SECONDS=retention_seconds)
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
        projected = conn.execute("SELECT COUNT(*) FROM check_runs_current").fetchone()[
            0
        ]

    assert remaining == 1
    assert projected == 1


def test_prune_old_projections_uses_completed_and_active_windows(tmp_path):
    db_path = tmp_path / "webhooks.sqlite3"
    store = _make_store(db_path)
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

    store = _make_store(
        db_path,
        WEBHOOK_PROJECTION_PRUNE_ENABLED=True,
        WEBHOOK_PROJECTION_COMPLETED_RETENTION_SECONDS=3600,
        WEBHOOK_PROJECTION_ACTIVE_RETENTION_SECONDS=7200,
    )
    counts = store.prune_old_projections()

    assert counts["check_runs_current:completed"] == 1
    assert counts["check_runs_current:active"] == 1
    assert counts["commit_status_current:completed"] == 1
    assert counts["commit_status_current:active"] == 0
    assert counts["pr_heads_current:completed"] == 1
    assert counts["pr_heads_current:active"] == 0

    with sqlite3.connect(str(db_path)) as conn:
        run_count = conn.execute("SELECT COUNT(*) FROM check_runs_current").fetchone()[
            0
        ]
        status_count = conn.execute(
            "SELECT COUNT(*) FROM commit_status_current"
        ).fetchone()[0]
        pr_count = conn.execute("SELECT COUNT(*) FROM pr_heads_current").fetchone()[0]

    assert run_count == 1
    assert status_count == 1
    assert pr_count == 1


def test_dashboard_rows_join_pagination_and_filter(tmp_path):
    db_path = tmp_path / "webhooks.sqlite3"
    store = _make_store(db_path)
    store.initialize()

    store.persist_event(
        delivery_id="pr-a",
        event="pull_request",
        payload={
            "action": "synchronize",
            "installation": {"id": 1},
            "repository": {"id": 1, "full_name": "org/repo-a"},
            "pull_request": {
                "id": 10,
                "number": 1,
                "title": "Repo A PR",
                "state": "open",
                "updated_at": "2026-02-18T10:00:00Z",
                "head": {"sha": "a" * 40},
                "base": {"ref": "main"},
            },
        },
        payload_json="{}",
    )
    store.persist_event(
        delivery_id="pr-a-closed",
        event="pull_request",
        payload={
            "action": "closed",
            "installation": {"id": 1},
            "repository": {"id": 1, "full_name": "org/repo-a"},
            "pull_request": {
                "id": 10,
                "number": 1,
                "title": "Repo A PR",
                "state": "closed",
                "updated_at": "2026-02-18T10:30:00Z",
                "head": {"sha": "a" * 40},
                "base": {"ref": "main"},
            },
        },
        payload_json="{}",
    )
    store.persist_event(
        delivery_id="pr-b",
        event="pull_request",
        payload={
            "action": "opened",
            "installation": {"id": 1},
            "repository": {"id": 2, "full_name": "org/repo-b"},
            "pull_request": {
                "id": 20,
                "number": 2,
                "title": "Repo B PR",
                "draft": True,
                "state": "open",
                "updated_at": "2026-02-18T11:00:00Z",
                "head": {"sha": "b" * 40},
                "base": {"ref": "main"},
            },
        },
        payload_json="{}",
    )

    store.upsert_sentinel_check_run(
        repo_id=2,
        repo_full_name="org/repo-b",
        check_run_id=12345,
        head_sha="b" * 40,
        name="merge-sentinel",
        status="completed",
        conclusion="success",
        app_id=999,
        started_at="2026-02-18T10:58:00Z",
        completed_at="2026-02-18T11:00:00Z",
        output_title="All checks green",
        output_summary=":white_check_mark: successful required checks: Build / test",
        output_text="# Checks\n\n| Check | Status | Required? |",
        output_checks_json='[{"name":"Build / test","status":"success","required":true}]',
        output_summary_hash="h1",
        output_text_hash="h2",
        last_eval_at="2026-02-18T11:00:01Z",
        last_publish_at="2026-02-18T11:00:02Z",
        last_publish_result="published",
        last_publish_error=None,
        last_delivery_id="eval-1",
    )

    assert store.count_pr_dashboard_rows() == 2
    assert store.count_pr_dashboard_rows(include_closed=False) == 1
    assert store.count_pr_dashboard_rows(repo_full_name="org/repo-b") == 1

    page1 = store.list_pr_dashboard_rows(
        app_id=999,
        check_name="merge-sentinel",
        page=1,
        page_size=1,
    )
    assert len(page1) == 1
    assert page1[0].repo_full_name == "org/repo-b"
    assert page1[0].pr_title == "Repo B PR"
    assert page1[0].pr_is_draft is True
    assert page1[0].output_summary is not None

    page2 = store.list_pr_dashboard_rows(
        app_id=999,
        check_name="merge-sentinel",
        page=2,
        page_size=1,
    )
    assert len(page2) == 1
    assert page2[0].repo_full_name == "org/repo-a"
    assert page2[0].pr_title == "Repo A PR"
    assert page2[0].sentinel_status is None

    filtered = store.list_pr_dashboard_rows(
        app_id=999,
        check_name="merge-sentinel",
        page=1,
        page_size=10,
        repo_full_name="org/repo-a",
    )
    assert len(filtered) == 1
    assert filtered[0].repo_full_name == "org/repo-a"

    open_only = store.list_pr_dashboard_rows(
        app_id=999,
        check_name="merge-sentinel",
        page=1,
        page_size=10,
        include_closed=False,
    )
    assert len(open_only) == 1
    assert open_only[0].repo_full_name == "org/repo-b"


def test_dashboard_rows_include_pr_merged_flag_from_last_pr_event(tmp_path):
    db_path = tmp_path / "webhooks.sqlite3"
    store = _make_store(db_path)
    store.initialize()

    merged_payload = make_pull_request_payload(
        head_sha="a" * 40,
        action="closed",
        state="closed",
        merged=True,
    )
    unmerged_payload = {
        "action": "closed",
        "installation": {"id": 13},
        "repository": {"id": 103, "full_name": "org/repo"},
        "pull_request": {
            "id": 6001,
            "number": 43,
            "title": "Unmerged close",
            "state": "closed",
            "merged": False,
            "updated_at": "2026-02-16T10:00:00Z",
            "head": {"sha": "b" * 40},
            "base": {"ref": "main"},
        },
    }
    store.persist_event(
        delivery_id="pr-merged-1",
        event="pull_request",
        payload=merged_payload,
        payload_json=store.payload_to_json(merged_payload),
    )
    store.persist_event(
        delivery_id="pr-unmerged-1",
        event="pull_request",
        payload=unmerged_payload,
        payload_json=store.payload_to_json(unmerged_payload),
    )

    rows = store.list_pr_dashboard_rows(
        app_id=999,
        check_name="merge-sentinel",
        page=1,
        page_size=10,
    )
    by_pr = {row.pr_number: row for row in rows}
    assert by_pr[42].pr_merged is True
    assert by_pr[43].pr_merged is False


def test_pr_detail_row_and_related_events_are_filtered_and_sorted(tmp_path):
    db_path = tmp_path / "webhooks.sqlite3"
    store = _make_store(db_path)
    store.initialize()

    head_sha = "c" * 40
    tracked_pr_payload = make_pull_request_payload(
        head_sha=head_sha, title="Tracked PR"
    )
    store.persist_event(
        delivery_id="pr-1",
        event="pull_request",
        payload=tracked_pr_payload,
        payload_json=store.payload_to_json(tracked_pr_payload),
    )
    check_run_payload = {
        "action": "completed",
        "installation": {"id": 11},
        "repository": {"id": 103, "full_name": "org/repo"},
        "check_run": {
            "id": 3001,
            "head_sha": head_sha,
            "name": "Builds / tests",
            "status": "completed",
            "conclusion": "success",
            "app": {"id": 1, "slug": "ci"},
            "check_suite": {"id": 77},
        },
    }
    store.persist_event(
        delivery_id="cr-1",
        event="check_run",
        payload=check_run_payload,
        payload_json=store.payload_to_json(check_run_payload),
    )
    status_payload = {
        "id": 901,
        "sha": head_sha,
        "context": "lint",
        "state": "success",
        "installation": {"id": 11},
        "repository": {"id": 103, "full_name": "org/repo"},
    }
    store.persist_event(
        delivery_id="status-1",
        event="status",
        payload=status_payload,
        payload_json=store.payload_to_json(status_payload),
    )
    other_pr_payload = {
        "action": "opened",
        "installation": {"id": 11},
        "repository": {"id": 103, "full_name": "org/repo"},
        "pull_request": {
            "id": 5002,
            "number": 99,
            "title": "Other PR",
            "state": "open",
            "updated_at": "2026-02-16T10:30:00Z",
            "head": {"sha": "d" * 40},
            "base": {"ref": "main"},
        },
    }
    store.persist_event(
        delivery_id="pr-other",
        event="pull_request",
        payload=other_pr_payload,
        payload_json=store.payload_to_json(other_pr_payload),
    )

    row = store.get_pr_dashboard_row(
        app_id=999,
        check_name="merge-sentinel",
        repo_id=103,
        pr_number=42,
    )
    assert row is not None
    assert row.pr_title == "Tracked PR"

    events = store.list_pr_related_events(
        repo_id=103,
        pr_number=42,
        head_sha=head_sha,
        limit=10,
    )
    assert [event.delivery_id for event in events] == ["status-1", "cr-1", "pr-1"]


def test_get_webhook_event_returns_decoded_payload(tmp_path):
    db_path = tmp_path / "webhooks.sqlite3"
    store = _make_store(db_path)
    store.initialize()

    payload = make_check_run_payload(conclusion="success")
    store.persist_event(
        delivery_id="delivery-123",
        event="check_run",
        payload=payload,
        payload_json=store.payload_to_json(payload),
    )

    event = store.get_webhook_event("delivery-123")
    assert event is not None
    assert event.delivery_id == "delivery-123"
    assert event.event == "check_run"
    assert event.payload["check_run"]["id"] == 2001
    assert event.detail is not None
    assert "tests" in event.detail


def test_pr_related_events_include_projection_activity(tmp_path):
    db_path = tmp_path / "webhooks.sqlite3"
    store = _make_store(db_path)
    store.initialize()

    head_sha = "a" * 40
    pr_payload = make_pull_request_payload(head_sha=head_sha, title="Tracked PR")
    store.persist_event(
        delivery_id="pr-1",
        event="pull_request",
        payload=pr_payload,
        payload_json=store.payload_to_json(pr_payload),
    )
    store.persist_event(
        delivery_id="cr-1",
        event="check_run",
        payload=make_check_run_payload(),
        payload_json=store.payload_to_json(make_check_run_payload()),
    )
    store.record_projection_activity_event(
        repo_id=103,
        repo_full_name="org/repo",
        pr_number=42,
        head_sha=head_sha,
        delivery_id="cr-1",
        activity_type="publish",
        result="dry_run",
        detail="Would publish completed/success",
        metadata={"status": "completed"},
        recorded_at="2030-01-01T00:00:00.000000Z",
    )

    events = store.list_pr_related_events(
        repo_id=103,
        pr_number=42,
        head_sha=head_sha,
        limit=10,
    )
    assert events[0].event == "sentinel"
    assert events[0].action == "publish"
    assert events[0].delivery_id == "cr-1"
    assert events[0].payload["status"] == "completed"


def test_pr_related_events_do_not_require_payload_decode(tmp_path):
    db_path = tmp_path / "webhooks.sqlite3"
    store = _make_store(db_path)
    store.initialize()

    head_sha = "a" * 40
    pr_payload = make_pull_request_payload(head_sha=head_sha, title="Tracked PR")
    store.persist_event(
        delivery_id="pr-1",
        event="pull_request",
        payload=pr_payload,
        payload_json=store.payload_to_json(pr_payload),
    )
    check_run_payload = make_check_run_payload()
    check_run_payload["check_run"]["head_sha"] = head_sha
    check_run_payload["repository"] = {"id": 103, "full_name": "org/repo"}
    store.persist_event(
        delivery_id="cr-1",
        event="check_run",
        payload=check_run_payload,
        payload_json=store.payload_to_json(check_run_payload),
    )

    with sqlite3.connect(str(db_path)) as conn:
        conn.execute(
            "UPDATE webhook_events SET payload_json = ? WHERE delivery_id = ?",
            (b"not-json-and-not-zstd", "cr-1"),
        )
        conn.commit()

    events = store.list_pr_related_events(
        repo_id=103,
        pr_number=42,
        head_sha=head_sha,
        limit=10,
    )
    assert [event.delivery_id for event in events] == ["cr-1", "pr-1"]
    assert events[0].detail is not None
    assert "tests" in events[0].detail


def test_prune_old_activity_events(tmp_path):
    db_path = tmp_path / "webhooks.sqlite3"
    store = _make_store(db_path, WEBHOOK_ACTIVITY_RETENTION_SECONDS=60)
    store.initialize()
    store.record_projection_activity_event(
        repo_id=103,
        repo_full_name="org/repo",
        pr_number=42,
        head_sha="a" * 40,
        delivery_id="d-new",
        activity_type="publish",
        result="dry_run",
        recorded_at=datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%fZ"),
    )
    old_recorded_at = (datetime.now(timezone.utc) - timedelta(seconds=3600)).strftime(
        "%Y-%m-%dT%H:%M:%S.%fZ"
    )
    store.record_projection_activity_event(
        repo_id=103,
        repo_full_name="org/repo",
        pr_number=42,
        head_sha="a" * 40,
        delivery_id="d-old",
        activity_type="publish",
        result="dry_run",
        recorded_at=old_recorded_at,
    )

    store = _make_store(
        db_path,
        WEBHOOK_ACTIVITY_RETENTION_SECONDS=300,
    )
    pruned = store.prune_old_activity_events()
    assert pruned == 1

    with sqlite3.connect(str(db_path)) as conn:
        rows = conn.execute(
            "SELECT delivery_id FROM sentinel_activity_events ORDER BY activity_id"
        ).fetchall()
    assert rows == [("d-new",)]
