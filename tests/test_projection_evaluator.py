from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
import hashlib
from pathlib import Path
import sqlite3

import pytest

from sentinel.config import SETTINGS
from sentinel.github.model import (
    ActionsRun,
    App,
    CheckRun,
    CommitSha,
    CommitStatus,
    PartialCheckSuite,
    Repository,
)
from sentinel.projection import ProjectionEvaluator, ProjectionTrigger
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


@dataclass
class _FakeContent:
    raw: str

    def decoded_content(self) -> str:
        return self.raw


class _FakeAPI:
    def __init__(
        self,
        *,
        config_yaml: str,
        post_id: int | None = None,
        on_get_content=None,
    ):
        self._config_yaml = config_yaml
        self._post_id = post_id
        self._on_get_content = on_get_content
        self.post_calls = []
        self.lookup_calls = 0
        self.get_content_calls = 0

    async def get_content(self, _repo_url: str, _path: str):
        self.get_content_calls += 1
        callback = self._on_get_content
        if callback is not None:
            self._on_get_content = None
            callback()
        return _FakeContent(self._config_yaml)

    async def get_pull(self, _repo_url: str, _number: int):  # pragma: no cover
        raise AssertionError("get_pull should not be called in this test")

    async def get_pull_request_files(self, _pr):  # pragma: no cover
        raise AssertionError("get_pull_request_files should not be called in this test")
        yield  # noqa: B018

    async def get_repository(self, _repo_url: str):  # pragma: no cover
        raise AssertionError("get_repository should not be called in this test")

    async def get_check_runs_for_ref(self, _repo, _ref):  # pragma: no cover
        raise AssertionError("get_check_runs_for_ref should not be called in this test")
        yield  # noqa: B018

    async def get_workflow_runs_for_ref(self, _repo, _ref):  # pragma: no cover
        raise AssertionError(
            "get_workflow_runs_for_ref should not be called in this test"
        )
        yield  # noqa: B018

    async def get_status_for_ref(self, _repo, _ref):  # pragma: no cover
        raise AssertionError("get_status_for_ref should not be called in this test")
        yield  # noqa: B018

    async def find_existing_sentinel_check_run(
        self,
        *,
        repo_url: str,
        head_sha: str,
        check_name: str,
        app_id: int | None = None,
    ):
        self.lookup_calls += 1
        return None

    async def post_check_run(self, repo_url: str, check_run):
        self.post_calls.append((repo_url, check_run))
        return self._post_id


class _FakeAPIRefresh(_FakeAPI):
    async def get_repository(self, _repo_url: str):
        return Repository(
            id=11,
            name="repo",
            full_name="org/repo",
            url="https://api.github.com/repos/org/repo",
            html_url="https://github.com/org/repo",
            private=False,
        )

    async def get_check_runs_for_ref(self, _repo, _ref):
        yield CheckRun(
            id=7001,
            name="tests",
            head_sha=CommitSha("a" * 40),
            status="completed",
            conclusion="success",
            started_at=datetime(2026, 2, 17, 10, 0, tzinfo=timezone.utc),
            completed_at=datetime(2026, 2, 17, 10, 1, tzinfo=timezone.utc),
            app=App(id=1, slug="ci"),
            check_suite=PartialCheckSuite(id=9901),
        )

    async def get_workflow_runs_for_ref(self, _repo, _ref):
        yield ActionsRun(
            id=8001,
            name="Builds",
            head_sha="a" * 40,
            run_number=10,
            event="pull_request",
            status="completed",
            conclusion="success",
            workflow_id=123,
            check_suite_id=9901,
            created_at=datetime(2026, 2, 17, 10, 0, tzinfo=timezone.utc),
            updated_at=datetime(2026, 2, 17, 10, 1, tzinfo=timezone.utc),
        )

    async def get_status_for_ref(self, _repo, _ref):
        yield CommitStatus(
            id=8801,
            url="https://api.github.com/repos/org/repo/statuses/8801",
            sha="a" * 40,
            context="lint",
            state="success",
            created_at=datetime(2026, 2, 17, 10, 0, tzinfo=timezone.utc),
            updated_at=datetime(2026, 2, 17, 10, 1, tzinfo=timezone.utc),
        )


class _FakeAPIRefreshTracking(_FakeAPIRefresh):
    def __init__(self, *, config_yaml: str):
        super().__init__(config_yaml=config_yaml)
        self.refresh_calls = 0

    async def get_repository(self, _repo_url: str):
        self.refresh_calls += 1
        return await super().get_repository(_repo_url)


def _check_run_payload(conclusion: str = "success") -> dict:
    return _check_run_payload_with_status(status="completed", conclusion=conclusion)


def _check_run_payload_with_status(
    *, status: str, conclusion: str | None = "success"
) -> dict:
    resolved_conclusion = conclusion if status == "completed" else None
    completed_at = "2026-02-17T10:01:00Z" if status == "completed" else None
    return {
        "action": "completed",
        "installation": {"id": 321},
        "repository": {"id": 11, "full_name": "org/repo"},
        "check_run": {
            "id": 7001,
            "head_sha": "a" * 40,
            "name": "tests",
            "status": status,
            "conclusion": resolved_conclusion,
            "started_at": "2026-02-17T10:00:00Z",
            "completed_at": completed_at,
            "app": {"id": 1, "slug": "ci"},
            "check_suite": {"id": 9901},
        },
    }


def _pull_request_payload(
    *,
    action: str = "synchronize",
    state: str = "open",
    head_sha: str = "a" * 40,
    draft: bool | None = None,
) -> dict:
    pull_request_payload = {
        "id": 5001,
        "number": 42,
        "state": state,
        "updated_at": "2026-02-17T10:02:00Z",
        "head": {"sha": head_sha},
        "base": {"ref": "main"},
    }
    if draft is not None:
        pull_request_payload["draft"] = draft

    return {
        "action": action,
        "installation": {"id": 321},
        "repository": {"id": 11, "full_name": "org/repo"},
        "pull_request": pull_request_payload,
    }


def _workflow_payload() -> dict:
    return {
        "action": "requested",
        "installation": {"id": 321},
        "repository": {"id": 11, "full_name": "org/repo"},
        "workflow_run": {
            "id": 8001,
            "name": "Builds",
            "event": "pull_request",
            "status": "completed",
            "conclusion": "success",
            "head_sha": "a" * 40,
            "run_number": 10,
            "workflow_id": 123,
            "check_suite_id": 9901,
            "app": {"id": 1, "slug": "github-actions"},
            "created_at": "2026-02-17T10:00:00Z",
            "updated_at": "2026-02-17T10:01:00Z",
        },
    }


async def _seed(store: WebhookStore) -> None:
    store.persist_event(
        delivery_id="pr-1",
        event="pull_request",
        payload=_pull_request_payload(),
        payload_json="{}",
    )
    store.persist_event(
        delivery_id="cr-1",
        event="check_run",
        payload=_check_run_payload(),
        payload_json="{}",
    )
    store.persist_event(
        delivery_id="wf-1",
        event="workflow_run",
        payload=_workflow_payload(),
        payload_json="{}",
    )


@dataclass
class _EvaluatorConfig:
    GITHUB_APP_ID: int = 2877723
    PROJECTION_CHECK_RUN_NAME: str = "merge-sentinel"
    PROJECTION_PUBLISH_ENABLED: bool = False
    PROJECTION_MANUAL_REFRESH_ACTION_ENABLED: bool = True
    PROJECTION_MANUAL_REFRESH_ACTION_IDENTIFIER: str = "refresh_from_api"
    PROJECTION_MANUAL_REFRESH_ACTION_LABEL: str = "Re-evaluate now"
    PROJECTION_MANUAL_REFRESH_ACTION_DESCRIPTION: str = (
        "Force refresh checks from GitHub and re-evaluate"
    )
    PROJECTION_PATH_RULE_FALLBACK_ENABLED: bool = True
    PROJECTION_AUTO_REFRESH_ON_MISSING_ENABLED: bool = True
    PROJECTION_AUTO_REFRESH_ON_MISSING_STALE_SECONDS: int = 1800
    PROJECTION_AUTO_REFRESH_ON_MISSING_COOLDOWN_SECONDS: int = 300
    PROJECTION_CONFIG_CACHE_SECONDS: int = 300
    PROJECTION_PR_FILES_CACHE_SECONDS: int = 86400


def _make_evaluator(
    *,
    store: WebhookStore,
    api_factory,
    publish_enabled: bool = False,
    stale_seconds: int = 1800,
) -> ProjectionEvaluator:
    return ProjectionEvaluator(
        store=store,
        config=_EvaluatorConfig(
            PROJECTION_PUBLISH_ENABLED=publish_enabled,
            PROJECTION_AUTO_REFRESH_ON_MISSING_STALE_SECONDS=stale_seconds,
        ),
        api_factory=api_factory,
    )


@pytest.mark.asyncio
async def test_projection_dry_run_persists_sentinel_row(tmp_path):
    store = _make_store(tmp_path / "webhooks.sqlite3")
    store.initialize()
    await _seed(store)

    api = _FakeAPI(
        config_yaml="rules:\n  - required_checks: ['Builds / tests']\n",
        post_id=9001,
    )

    async def api_factory(_installation: int):
        return api

    evaluator = _make_evaluator(store=store, api_factory=api_factory)

    result = await evaluator.evaluate_and_publish(
        ProjectionTrigger(
            repo_id=11,
            repo_full_name="org/repo",
            head_sha="a" * 40,
            installation_id=321,
            delivery_id="d-1",
            event="check_run",
        )
    )

    assert result.result == "dry_run"
    assert result.check_run_id is None
    assert api.post_calls == []

    with sqlite3.connect(str(tmp_path / "webhooks.sqlite3")) as conn:
        row = conn.execute(
            """
            SELECT status, conclusion, output_title, output_summary, output_text,
                   output_summary_hash, output_text_hash, last_publish_result, app_id, check_run_id
            FROM sentinel_check_run_state
            WHERE repo_id = ? AND head_sha = ? AND check_name = ?
            LIMIT 1
            """,
            (11, "a" * 40, "merge-sentinel"),
        ).fetchone()

    assert row == (
        "completed",
        "success",
        "All 1 required jobs successful",
        ":white_check_mark: successful required checks: Builds / tests",
        "# Checks\n\n| Check | Status | Required? |\n| --- | --- | --- |\n| Builds / tests | success | yes |",
        hashlib.sha256(
            ":white_check_mark: successful required checks: Builds / tests".encode(
                "utf-8"
            )
        ).hexdigest(),
        hashlib.sha256(
            "# Checks\n\n| Check | Status | Required? |\n| --- | --- | --- |\n| Builds / tests | success | yes |".encode(
                "utf-8"
            )
        ).hexdigest(),
        "dry_run",
        2877723,
        None,
    )

    with sqlite3.connect(str(tmp_path / "webhooks.sqlite3")) as conn:
        activity_rows = conn.execute(
            """
            SELECT activity_type, result
            FROM sentinel_activity_events
            WHERE repo_id = ? AND pr_number = ? AND head_sha = ?
            ORDER BY activity_id
            """,
            (11, 42, "a" * 40),
        ).fetchall()

    assert ("config_fetch", "cache_miss") in activity_rows
    assert ("config_fetch", "loaded") in activity_rows
    assert ("publish", "dry_run") in activity_rows


@pytest.mark.asyncio
async def test_projection_pre_delay_pending_emits_in_progress_before_full_eval(
    tmp_path,
):
    store = _make_store(tmp_path / "webhooks.sqlite3")
    store.initialize()
    store.persist_event(
        delivery_id="pr-1",
        event="pull_request",
        payload=_pull_request_payload(),
        payload_json="{}",
    )

    api = _FakeAPI(
        config_yaml="rules:\n  - required_checks: ['Builds / tests']\n",
        post_id=9001,
    )

    async def api_factory(_installation: int):
        return api

    evaluator = _make_evaluator(store=store, api_factory=api_factory)
    result = await evaluator.evaluate_and_publish(
        ProjectionTrigger(
            repo_id=11,
            repo_full_name="org/repo",
            head_sha="a" * 40,
            installation_id=321,
            delivery_id="d-pre-delay-1",
            event="pull_request",
            action="synchronize",
            pre_delay_pending=True,
        )
    )

    assert result.result == "pre_delay_pending"
    assert api.get_content_calls == 0
    assert api.post_calls == []

    with sqlite3.connect(str(tmp_path / "webhooks.sqlite3")) as conn:
        row = conn.execute(
            """
            SELECT status, conclusion, output_title, last_publish_result
            FROM sentinel_check_run_state
            WHERE repo_id = ? AND head_sha = ? AND check_name = ?
            LIMIT 1
            """,
            (11, "a" * 40, "merge-sentinel"),
        ).fetchone()

    assert row == (
        "in_progress",
        None,
        "Waiting briefly for checks after PR update",
        "dry_run",
    )

    with sqlite3.connect(str(tmp_path / "webhooks.sqlite3")) as conn:
        activity_rows = conn.execute(
            """
            SELECT activity_type, result
            FROM sentinel_activity_events
            WHERE repo_id = ? AND pr_number = ? AND head_sha = ?
            ORDER BY activity_id
            """,
            (11, 42, "a" * 40),
        ).fetchall()
    assert ("publish_pre_delay", "dry_run") in activity_rows


@pytest.mark.asyncio
async def test_projection_dry_run_marks_draft_pr_in_activity_detail(tmp_path):
    store = _make_store(tmp_path / "webhooks.sqlite3")
    store.initialize()
    store.persist_event(
        delivery_id="pr-1",
        event="pull_request",
        payload=_pull_request_payload(draft=True),
        payload_json="{}",
    )
    store.persist_event(
        delivery_id="cr-1",
        event="check_run",
        payload=_check_run_payload(),
        payload_json="{}",
    )
    store.persist_event(
        delivery_id="wf-1",
        event="workflow_run",
        payload=_workflow_payload(),
        payload_json="{}",
    )

    api = _FakeAPI(
        config_yaml="rules:\n  - required_checks: ['Builds / tests']\n",
        post_id=9001,
    )

    async def api_factory(_installation: int):
        return api

    evaluator = _make_evaluator(store=store, api_factory=api_factory)

    result = await evaluator.evaluate_and_publish(
        ProjectionTrigger(
            repo_id=11,
            repo_full_name="org/repo",
            head_sha="a" * 40,
            installation_id=321,
            delivery_id="d-dry-draft-1",
            event="check_run",
        )
    )

    assert result.result == "dry_run"
    assert api.post_calls == []

    with sqlite3.connect(str(tmp_path / "webhooks.sqlite3")) as conn:
        publish_rows = conn.execute(
            """
            SELECT activity_type, result, detail
            FROM sentinel_activity_events
            WHERE repo_id = ? AND pr_number = ? AND head_sha = ?
              AND activity_type = 'publish'
            ORDER BY activity_id
            """,
            (11, 42, "a" * 40),
        ).fetchall()

    assert (
        "publish",
        "dry_run",
        "Draft PR, would not publish completed/success (dry-run)",
    ) in publish_rows


@pytest.mark.asyncio
async def test_projection_second_identical_eval_is_unchanged(tmp_path):
    store = _make_store(tmp_path / "webhooks.sqlite3")
    store.initialize()
    await _seed(store)

    api = _FakeAPI(config_yaml="rules:\n  - required_checks: ['Builds / tests']\n")

    async def api_factory(_installation: int):
        return api

    evaluator = _make_evaluator(store=store, api_factory=api_factory)
    trigger = ProjectionTrigger(
        repo_id=11,
        repo_full_name="org/repo",
        head_sha="a" * 40,
        installation_id=321,
        delivery_id="d-2",
        event="check_run",
    )

    first = await evaluator.evaluate_and_publish(trigger)
    second = await evaluator.evaluate_and_publish(trigger)

    assert first.result == "dry_run"
    assert second.result == "unchanged"

    with sqlite3.connect(str(tmp_path / "webhooks.sqlite3")) as conn:
        row = conn.execute(
            """
            SELECT output_summary, output_text, output_summary_hash, output_text_hash
            FROM sentinel_check_run_state
            WHERE repo_id = ? AND head_sha = ? AND check_name = ?
            """,
            (11, "a" * 40, "merge-sentinel"),
        ).fetchone()
    assert row is not None
    assert row[0] is not None
    assert row[1] is not None
    assert row[2] == hashlib.sha256(row[0].encode("utf-8")).hexdigest()
    assert row[3] == hashlib.sha256(row[1].encode("utf-8")).hexdigest()


@pytest.mark.asyncio
async def test_projection_publish_persists_real_id(tmp_path):
    store = _make_store(tmp_path / "webhooks.sqlite3")
    store.initialize()
    await _seed(store)

    api = _FakeAPI(
        config_yaml="rules:\n  - required_checks: ['Builds / tests']\n",
        post_id=91234,
    )

    async def api_factory(_installation: int):
        return api

    evaluator = _make_evaluator(
        store=store, api_factory=api_factory, publish_enabled=True
    )

    result = await evaluator.evaluate_and_publish(
        ProjectionTrigger(
            repo_id=11,
            repo_full_name="org/repo",
            head_sha="a" * 40,
            installation_id=321,
            delivery_id="d-3",
            event="check_run",
        )
    )

    assert result.result == "published"
    assert api.lookup_calls == 1
    assert len(api.post_calls) == 1
    posted_check_run = api.post_calls[0][1]
    assert [action.identifier for action in posted_check_run.actions] == [
        "refresh_from_api"
    ]

    with sqlite3.connect(str(tmp_path / "webhooks.sqlite3")) as conn:
        rows = conn.execute(
            """
            SELECT check_run_id, last_publish_result
            FROM sentinel_check_run_state
            WHERE repo_id = ? AND head_sha = ? AND check_name = ? AND app_id = ?
            """,
            (11, "a" * 40, "merge-sentinel", 2877723),
        ).fetchall()

    assert rows == [(91234, "published")]


@pytest.mark.asyncio
async def test_projection_publish_failure_to_success_forces_new_run(tmp_path):
    store = _make_store(tmp_path / "webhooks.sqlite3")
    store.initialize()
    await _seed(store)
    store.persist_event(
        delivery_id="cr-fail-1",
        event="check_run",
        payload=_check_run_payload(conclusion="failure"),
        payload_json="{}",
    )

    api = _FakeAPI(
        config_yaml="rules:\n  - required_checks: ['Builds / tests']\n",
        post_id=91234,
    )

    async def api_factory(_installation: int):
        return api

    evaluator = _make_evaluator(
        store=store, api_factory=api_factory, publish_enabled=True
    )

    first = await evaluator.evaluate_and_publish(
        ProjectionTrigger(
            repo_id=11,
            repo_full_name="org/repo",
            head_sha="a" * 40,
            installation_id=321,
            delivery_id="d-fail-1",
            event="check_run",
        )
    )
    assert first.result == "published"
    assert len(api.post_calls) == 1
    assert api.post_calls[0][1].id is None

    store.persist_event(
        delivery_id="cr-success-1",
        event="check_run",
        payload=_check_run_payload(conclusion="success"),
        payload_json="{}",
    )

    second = await evaluator.evaluate_and_publish(
        ProjectionTrigger(
            repo_id=11,
            repo_full_name="org/repo",
            head_sha="a" * 40,
            installation_id=321,
            delivery_id="d-success-1",
            event="check_run",
        )
    )

    assert second.result == "published"
    assert len(api.post_calls) == 2
    assert api.post_calls[1][1].id is None
    assert api.post_calls[1][1].status == "completed"
    assert api.post_calls[1][1].conclusion == "success"
    # first publish needs a lookup miss; second should force create without lookup.
    assert api.lookup_calls == 1


@pytest.mark.asyncio
async def test_projection_publish_skips_for_draft_pr(tmp_path):
    store = _make_store(tmp_path / "webhooks.sqlite3")
    store.initialize()
    store.persist_event(
        delivery_id="pr-1",
        event="pull_request",
        payload=_pull_request_payload(draft=True),
        payload_json="{}",
    )
    store.persist_event(
        delivery_id="cr-1",
        event="check_run",
        payload=_check_run_payload(),
        payload_json="{}",
    )
    store.persist_event(
        delivery_id="wf-1",
        event="workflow_run",
        payload=_workflow_payload(),
        payload_json="{}",
    )

    api = _FakeAPI(
        config_yaml="rules:\n  - required_checks: ['Builds / tests']\n",
        post_id=91234,
    )

    async def api_factory(_installation: int):
        return api

    evaluator = _make_evaluator(
        store=store, api_factory=api_factory, publish_enabled=True
    )

    result = await evaluator.evaluate_and_publish(
        ProjectionTrigger(
            repo_id=11,
            repo_full_name="org/repo",
            head_sha="a" * 40,
            installation_id=321,
            delivery_id="d-draft-1",
            event="check_run",
        )
    )

    assert result.result == "skipped_draft"
    assert api.lookup_calls == 0
    assert api.post_calls == []

    second_result = await evaluator.evaluate_and_publish(
        ProjectionTrigger(
            repo_id=11,
            repo_full_name="org/repo",
            head_sha="a" * 40,
            installation_id=321,
            delivery_id="d-draft-2",
            event="check_run",
        )
    )
    assert second_result.result == "unchanged"

    with sqlite3.connect(str(tmp_path / "webhooks.sqlite3")) as conn:
        rows = conn.execute(
            """
            SELECT check_run_id, last_publish_result
            FROM sentinel_check_run_state
            WHERE repo_id = ? AND head_sha = ? AND check_name = ? AND app_id = ?
            """,
            (11, "a" * 40, "merge-sentinel", 2877723),
        ).fetchall()

    assert rows == [(None, "unchanged")]

    with sqlite3.connect(str(tmp_path / "webhooks.sqlite3")) as conn:
        activity_rows = conn.execute(
            """
            SELECT activity_type, result, detail
            FROM sentinel_activity_events
            WHERE repo_id = ? AND pr_number = ? AND head_sha = ?
            ORDER BY activity_id
            """,
            (11, 42, "a" * 40),
        ).fetchall()
    assert (
        "publish",
        "skipped_draft",
        "Draft PR, would publish completed/success",
    ) in activity_rows
    assert (
        "publish",
        "unchanged",
        "Draft PR, publish suppressed (completed/success)",
    ) in activity_rows


@pytest.mark.asyncio
async def test_projection_publish_skips_when_pr_closed_during_evaluation(tmp_path):
    store = _make_store(tmp_path / "webhooks.sqlite3")
    store.initialize()
    await _seed(store)

    def close_pr() -> None:
        store.persist_event(
            delivery_id="pr-closed-1",
            event="pull_request",
            payload=_pull_request_payload(action="closed", state="closed"),
            payload_json="{}",
        )

    api = _FakeAPI(
        config_yaml="rules:\n  - required_checks: ['Builds / tests']\n",
        post_id=91234,
        on_get_content=close_pr,
    )

    async def api_factory(_installation: int):
        return api

    evaluator = _make_evaluator(
        store=store, api_factory=api_factory, publish_enabled=True
    )

    result = await evaluator.evaluate_and_publish(
        ProjectionTrigger(
            repo_id=11,
            repo_full_name="org/repo",
            head_sha="a" * 40,
            installation_id=321,
            delivery_id="d-4",
            event="check_run",
        )
    )

    assert result.result == "no_pr"
    assert api.post_calls == []

    with sqlite3.connect(str(tmp_path / "webhooks.sqlite3")) as conn:
        row_count = conn.execute(
            """
            SELECT COUNT(*)
            FROM sentinel_check_run_state
            WHERE repo_id = ? AND head_sha = ? AND check_name = ? AND app_id = ?
            """,
            (11, "a" * 40, "merge-sentinel", 2877723),
        ).fetchone()[0]

    assert row_count == 0


@pytest.mark.asyncio
async def test_force_api_refresh_persists_projection_rows(tmp_path):
    store = _make_store(tmp_path / "webhooks.sqlite3")
    store.initialize()
    store.persist_event(
        delivery_id="pr-1",
        event="pull_request",
        payload=_pull_request_payload(),
        payload_json="{}",
    )

    api = _FakeAPIRefresh(
        config_yaml="rules:\n  - required_checks: ['Builds / tests']\n"
    )

    async def api_factory(_installation: int):
        return api

    evaluator = _make_evaluator(store=store, api_factory=api_factory)

    result = await evaluator.evaluate_and_publish(
        ProjectionTrigger(
            repo_id=11,
            repo_full_name="org/repo",
            head_sha="a" * 40,
            installation_id=321,
            delivery_id="d-api-1",
            event="check_run",
            force_api_refresh=True,
        )
    )

    assert result.result == "dry_run"

    check_rows = store.get_check_runs_for_head(11, "a" * 40)
    assert len(check_rows) == 1
    assert check_rows[0].name == "tests"
    assert check_rows[0].last_delivery_id == "d-api-1"

    workflow_rows = store.get_workflow_runs_for_head(11, "a" * 40)
    assert len(workflow_rows) == 1
    assert workflow_rows[0].name == "Builds"
    assert workflow_rows[0].last_delivery_id == "d-api-1"

    status_rows = store.get_commit_statuses_for_sha(11, "a" * 40)
    assert len(status_rows) == 1
    assert status_rows[0].context == "lint"
    assert status_rows[0].last_delivery_id == "d-api-1"


@pytest.mark.asyncio
async def test_auto_refresh_on_missing_pattern_for_stale_pr(tmp_path):
    store = _make_store(tmp_path / "webhooks.sqlite3")
    store.initialize()
    store.persist_event(
        delivery_id="pr-1",
        event="pull_request",
        payload=_pull_request_payload(),
        payload_json="{}",
    )
    store.persist_event(
        delivery_id="cr-1",
        event="check_run",
        payload=_check_run_payload(),
        payload_json="{}",
    )

    api = _FakeAPIRefreshTracking(
        config_yaml="rules:\n  - required_pattern: ['Builds / *']\n"
    )

    async def api_factory(_installation: int):
        return api

    evaluator = _make_evaluator(
        store=store,
        api_factory=api_factory,
        stale_seconds=60,
    )

    result = await evaluator.evaluate_and_publish(
        ProjectionTrigger(
            repo_id=11,
            repo_full_name="org/repo",
            head_sha="a" * 40,
            installation_id=321,
            delivery_id="d-auto-refresh-1",
            event="check_run",
        )
    )

    assert result.result == "dry_run"
    assert api.refresh_calls == 1

    with sqlite3.connect(str(tmp_path / "webhooks.sqlite3")) as conn:
        row = conn.execute(
            """
            SELECT conclusion, output_summary
            FROM sentinel_check_run_state
            WHERE repo_id = ? AND head_sha = ? AND check_name = ?
            LIMIT 1
            """,
            (11, "a" * 40, "merge-sentinel"),
        ).fetchone()

    assert row is not None
    assert row[0] == "success"
    assert "Builds / tests" in row[1]

    with sqlite3.connect(str(tmp_path / "webhooks.sqlite3")) as conn:
        refresh_rows = conn.execute(
            """
            SELECT result
            FROM sentinel_activity_events
            WHERE repo_id = ? AND pr_number = ? AND head_sha = ?
              AND activity_type = 'missing_refresh'
            ORDER BY activity_id
            """,
            (11, 42, "a" * 40),
        ).fetchall()
    assert ("triggered",) in refresh_rows


@pytest.mark.asyncio
async def test_auto_refresh_on_stale_running_check(tmp_path):
    store = _make_store(tmp_path / "webhooks.sqlite3")
    store.initialize()
    store.persist_event(
        delivery_id="pr-1",
        event="pull_request",
        payload=_pull_request_payload(),
        payload_json="{}",
    )
    store.persist_event(
        delivery_id="cr-1",
        event="check_run",
        payload=_check_run_payload_with_status(status="in_progress"),
        payload_json="{}",
    )
    store.persist_event(
        delivery_id="wf-1",
        event="workflow_run",
        payload=_workflow_payload(),
        payload_json="{}",
    )

    old_last_seen_at = (datetime.now(timezone.utc) - timedelta(hours=2)).strftime(
        "%Y-%m-%dT%H:%M:%S.%fZ"
    )
    with sqlite3.connect(str(tmp_path / "webhooks.sqlite3")) as conn:
        conn.execute(
            """
            UPDATE check_runs_current
            SET last_seen_at = ?
            WHERE repo_id = ? AND check_run_id = ?
            """,
            (old_last_seen_at, 11, 7001),
        )
        conn.commit()

    api = _FakeAPIRefreshTracking(
        config_yaml="rules:\n  - required_checks: ['Builds / tests']\n"
    )

    async def api_factory(_installation: int):
        return api

    evaluator = _make_evaluator(
        store=store,
        api_factory=api_factory,
        stale_seconds=60,
    )

    result = await evaluator.evaluate_and_publish(
        ProjectionTrigger(
            repo_id=11,
            repo_full_name="org/repo",
            head_sha="a" * 40,
            installation_id=321,
            delivery_id="d-stale-running-1",
            event="check_run",
        )
    )

    assert result.result == "dry_run"
    assert api.refresh_calls == 1

    with sqlite3.connect(str(tmp_path / "webhooks.sqlite3")) as conn:
        refresh_rows = conn.execute(
            """
            SELECT result
            FROM sentinel_activity_events
            WHERE repo_id = ? AND pr_number = ? AND head_sha = ?
              AND activity_type = 'stale_running_refresh'
            ORDER BY activity_id
            """,
            (11, 42, "a" * 40),
        ).fetchall()
    assert ("triggered",) in refresh_rows
