from datetime import datetime, timezone
from pathlib import Path

from sentinel.config import SETTINGS
from sentinel.github.model import (
    ActionsRun,
    App,
    CheckRun,
    CommitSha,
    CommitStatus,
    PartialCheckSuite,
)
from sentinel.storage import WebhookStore
from sentinel.storage.types import CheckRunRow, CommitStatusRow, WorkflowRunRow


def _make_store(db_path, **updates):
    return WebhookStore(
        settings=SETTINGS.model_copy(
            update={
                "WEBHOOK_DB_PATH": Path(db_path),
                **updates,
            }
        )
    )


def test_check_run_row_from_github_check_run_maps_expected_fields():
    started_at = datetime(2026, 2, 19, 10, 0, tzinfo=timezone.utc)
    completed_at = datetime(2026, 2, 19, 10, 5, tzinfo=timezone.utc)
    check_run = CheckRun(
        id=1234,
        name="Build / tests",
        head_sha=CommitSha("a" * 40),
        status="completed",
        conclusion="success",
        started_at=started_at,
        completed_at=completed_at,
        app=App(id=99, slug="ci-app"),
        check_suite=PartialCheckSuite(id=777),
        html_url="https://github.com/org/repo/runs/1234",
    )

    row = CheckRunRow.from_github_check_run(check_run)

    assert row.check_run_id == 1234
    assert row.name == "Build / tests"
    assert row.head_sha == "a" * 40
    assert row.status == "completed"
    assert row.conclusion == "success"
    assert row.app_id == 99
    assert row.app_slug == "ci-app"
    assert row.check_suite_id == 777
    assert row.html_url == "https://github.com/org/repo/runs/1234"
    assert row.started_at == started_at
    assert row.completed_at == completed_at


def test_workflow_run_row_from_github_actions_run_maps_expected_fields():
    created_at = datetime(2026, 2, 19, 9, 0, tzinfo=timezone.utc)
    updated_at = datetime(2026, 2, 19, 9, 3, tzinfo=timezone.utc)
    workflow_run = ActionsRun(
        id=555,
        name="Builds",
        head_sha="b" * 40,
        run_number=22,
        event="pull_request",
        status="in_progress",
        conclusion=None,
        workflow_id=44,
        check_suite_id=66,
        created_at=created_at,
        updated_at=updated_at,
    )

    row = WorkflowRunRow.from_github_actions_run(workflow_run)

    assert row.workflow_run_id == 555
    assert row.name == "Builds"
    assert row.head_sha == "b" * 40
    assert row.run_number == 22
    assert row.event == "pull_request"
    assert row.status == "in_progress"
    assert row.conclusion is None
    assert row.workflow_id == 44
    assert row.check_suite_id == 66
    assert row.created_at == created_at
    assert row.updated_at == updated_at


def test_commit_status_row_from_github_commit_status_maps_expected_fields():
    created_at = datetime(2026, 2, 19, 8, 0, tzinfo=timezone.utc)
    updated_at = datetime(2026, 2, 19, 8, 1, tzinfo=timezone.utc)
    status = CommitStatus(
        url="https://api.github.com/repos/org/repo/statuses/123",
        id=9001,
        state="pending",
        created_at=created_at,
        updated_at=updated_at,
        sha="c" * 40,
        context="lint",
    )

    row = CommitStatusRow.from_github_commit_status(status)

    assert row.status_id == 9001
    assert row.url == "https://api.github.com/repos/org/repo/statuses/123"
    assert row.sha == "c" * 40
    assert row.context == "lint"
    assert row.state == "pending"
    assert row.created_at == created_at
    assert row.updated_at == updated_at


def test_upsert_head_snapshot_from_api_prefers_row_head_sha(tmp_path):
    store = _make_store(tmp_path / "webhooks.sqlite3")
    store.initialize()

    head_sha = "d" * 40
    fallback_sha = "e" * 40
    started_at = datetime(2026, 2, 19, 7, 0, tzinfo=timezone.utc)
    completed_at = datetime(2026, 2, 19, 7, 10, tzinfo=timezone.utc)

    check_row = CheckRunRow.from_github_check_run(
        CheckRun(
            id=42,
            name="tests",
            head_sha=CommitSha(head_sha),
            status="completed",
            conclusion="success",
            started_at=started_at,
            completed_at=completed_at,
        )
    )
    workflow_row = WorkflowRunRow.from_github_actions_run(
        ActionsRun(
            id=101,
            name="Builds",
            head_sha=head_sha,
            run_number=10,
            event="pull_request",
            status="completed",
            conclusion="success",
            workflow_id=7,
            check_suite_id=8,
            created_at=started_at,
            updated_at=completed_at,
        )
    )
    status_row = CommitStatusRow.from_github_commit_status(
        CommitStatus(
            url="https://api.github.com/repos/org/repo/statuses/1",
            id=1,
            state="success",
            created_at=started_at,
            updated_at=completed_at,
            sha=head_sha,
            context="lint",
        )
    )

    counts = store.upsert_head_snapshot_from_api(
        repo_id=100,
        repo_full_name="org/repo",
        head_sha=fallback_sha,
        delivery_id="api-refresh-1",
        check_rows=[check_row],
        workflow_rows=[workflow_row],
        status_rows=[status_row],
    )

    assert counts == {"check_runs": 1, "workflow_runs": 1, "commit_statuses": 1}
    assert len(store.get_check_runs_for_head(100, head_sha)) == 1
    assert len(store.get_workflow_runs_for_head(100, head_sha)) == 1
