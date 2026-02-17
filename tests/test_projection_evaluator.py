from __future__ import annotations

from dataclasses import dataclass
import sqlite3
from typing import Optional

import pytest

from sentinel.projection import ProjectionEvaluator, ProjectionTrigger
from sentinel.storage import WebhookStore


@dataclass
class _FakeContent:
    raw: str

    def decoded_content(self) -> str:
        return self.raw


class _FakeAPI:
    def __init__(self, *, config_yaml: str, post_id: Optional[int] = None):
        self._config_yaml = config_yaml
        self._post_id = post_id
        self.post_calls = []
        self.lookup_calls = 0

    async def get_content(self, _repo_url: str, _path: str):
        return _FakeContent(self._config_yaml)

    async def get_pull(self, _repo_url: str, _number: int):  # pragma: no cover
        raise AssertionError("get_pull should not be called in this test")

    async def get_pull_request_files(self, _pr):  # pragma: no cover
        raise AssertionError("get_pull_request_files should not be called in this test")
        yield  # noqa: B018

    async def find_existing_sentinel_check_run(
        self,
        *,
        repo_url: str,
        head_sha: str,
        check_name: str,
        app_id: Optional[int] = None,
    ):
        self.lookup_calls += 1
        return None

    async def post_check_run(self, repo_url: str, check_run):
        self.post_calls.append((repo_url, check_run))
        return self._post_id


def _check_run_payload(conclusion: str = "success") -> dict:
    return {
        "action": "completed",
        "installation": {"id": 321},
        "repository": {"id": 11, "full_name": "org/repo"},
        "check_run": {
            "id": 7001,
            "head_sha": "a" * 40,
            "name": "tests",
            "status": "completed",
            "conclusion": conclusion,
            "started_at": "2026-02-17T10:00:00Z",
            "completed_at": "2026-02-17T10:01:00Z",
            "app": {"id": 1, "slug": "ci"},
            "check_suite": {"id": 9901},
        },
    }


def _pull_request_payload() -> dict:
    return {
        "action": "synchronize",
        "installation": {"id": 321},
        "repository": {"id": 11, "full_name": "org/repo"},
        "pull_request": {
            "id": 5001,
            "number": 42,
            "state": "open",
            "updated_at": "2026-02-17T10:02:00Z",
            "head": {"sha": "a" * 40},
            "base": {"ref": "main"},
        },
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


@pytest.mark.asyncio
async def test_projection_dry_run_persists_sentinel_row(tmp_path):
    store = WebhookStore(str(tmp_path / "webhooks.sqlite3"))
    store.initialize()
    await _seed(store)

    api = _FakeAPI(
        config_yaml="rules:\n  - required_checks: ['Builds / tests']\n",
        post_id=9001,
    )

    async def api_factory(_installation: int):
        return api

    evaluator = ProjectionEvaluator(
        store=store,
        app_id=2877723,
        check_run_name="merge-sentinel",
        publish_enabled=False,
        path_rule_fallback_enabled=True,
        config_cache_seconds=300,
        pr_files_cache_seconds=86400,
        api_factory=api_factory,
    )

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
    assert api.post_calls == []

    with sqlite3.connect(str(tmp_path / "webhooks.sqlite3")) as conn:
        row = conn.execute(
            """
            SELECT status, conclusion, output_title, last_publish_result, app_id
            FROM check_runs_current
            WHERE repo_id = ? AND head_sha = ? AND name = ?
            ORDER BY last_seen_at DESC
            LIMIT 1
            """,
            (11, "a" * 40, "merge-sentinel"),
        ).fetchone()

    assert row == (
        "completed",
        "success",
        "All 1 required jobs successful",
        "dry_run",
        2877723,
    )


@pytest.mark.asyncio
async def test_projection_second_identical_eval_is_unchanged(tmp_path):
    store = WebhookStore(str(tmp_path / "webhooks.sqlite3"))
    store.initialize()
    await _seed(store)

    api = _FakeAPI(config_yaml="rules:\n  - required_checks: ['Builds / tests']\n")

    async def api_factory(_installation: int):
        return api

    evaluator = ProjectionEvaluator(
        store=store,
        app_id=2877723,
        check_run_name="merge-sentinel",
        publish_enabled=False,
        path_rule_fallback_enabled=True,
        config_cache_seconds=300,
        pr_files_cache_seconds=86400,
        api_factory=api_factory,
    )
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


@pytest.mark.asyncio
async def test_projection_publish_replaces_synthetic_id_with_real_id(tmp_path):
    store = WebhookStore(str(tmp_path / "webhooks.sqlite3"))
    store.initialize()
    await _seed(store)

    api = _FakeAPI(
        config_yaml="rules:\n  - required_checks: ['Builds / tests']\n",
        post_id=91234,
    )

    async def api_factory(_installation: int):
        return api

    evaluator = ProjectionEvaluator(
        store=store,
        app_id=2877723,
        check_run_name="merge-sentinel",
        publish_enabled=True,
        path_rule_fallback_enabled=True,
        config_cache_seconds=300,
        pr_files_cache_seconds=86400,
        api_factory=api_factory,
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

    with sqlite3.connect(str(tmp_path / "webhooks.sqlite3")) as conn:
        rows = conn.execute(
            """
            SELECT check_run_id, last_publish_result
            FROM check_runs_current
            WHERE repo_id = ? AND head_sha = ? AND name = ? AND app_id = ?
            """,
            (11, "a" * 40, "merge-sentinel", 2877723),
        ).fetchall()

    assert rows == [(91234, "published")]
