from types import SimpleNamespace
from pathlib import Path
from typing import cast

import pytest
from sanic import Request, Sanic

from sentinel.config import SETTINGS
from sentinel.storage import WebhookStore
from sentinel.state_dashboard import (
    StateUpdateBroadcaster,
    _pr_draft_chip_class,
    _pr_state_chip_class,
    _publish_result_display,
    _conclusion_chip_class,
    _state_dashboard_context,
    _state_event_detail_context,
    _state_pr_detail_context,
    _state_query_params,
    _status_chip_class,
)
from sentinel.web import create_app


def _make_store(db_path, **updates):
    return WebhookStore(
        settings=SETTINGS.model_copy(
            update={
                "WEBHOOK_DB_PATH": Path(db_path),
                **updates,
            }
        )
    )


def test_state_query_params_parses_and_clamps():
    class _FakeArgs:
        def __init__(self):
            self._data = {
                "page": ["-2"],
                "page_size": ["999"],
                "repo": [" org/repo "],
                "include_closed": ["0"],
            }

        def getlist(self, key):
            return list(self._data.get(key, []))

    request = SimpleNamespace(args=_FakeArgs())
    page, page_size, repo, include_closed = _state_query_params(cast(Request, request))
    assert page == 1
    assert page_size == 200
    assert repo == "org/repo"
    assert include_closed is False


def test_state_query_params_include_closed_uses_all_values_when_duplicated():
    class _FakeArgs:
        def __init__(self):
            self._data = {
                "page": ["1"],
                "page_size": ["100"],
                "repo": [""],
                "include_closed": ["0", "1"],
            }

        def get(self, key, default=None):
            values = self._data.get(key)
            if not values:
                return default
            return values[0]

        def getlist(self, key):
            return list(self._data.get(key, []))

    request = SimpleNamespace(args=_FakeArgs())
    page, page_size, repo, include_closed = _state_query_params(cast(Request, request))
    assert page == 1
    assert page_size == 100
    assert repo is None
    assert include_closed is True


def test_state_dashboard_context_includes_links_and_pagination(tmp_path):
    store = _make_store(tmp_path / "webhooks.sqlite3")
    store.initialize()

    store.persist_event(
        delivery_id="pr-1",
        event="pull_request",
        payload={
            "action": "synchronize",
            "installation": {"id": 1},
            "repository": {"id": 10, "full_name": "org/repo"},
            "pull_request": {
                "id": 1001,
                "number": 42,
                "title": "Add dashboard",
                "draft": True,
                "state": "open",
                "updated_at": "2026-02-18T12:00:00Z",
                "head": {"sha": "a" * 40},
                "base": {"ref": "main"},
            },
        },
        payload_json="{}",
    )
    store.upsert_sentinel_check_run(
        repo_id=10,
        repo_full_name="org/repo",
        check_run_id=321,
        head_sha="a" * 40,
        name="merge-sentinel",
        status="completed",
        conclusion="success",
        app_id=2877723,
        started_at="2026-02-18T11:59:00Z",
        completed_at="2026-02-18T12:00:00Z",
        output_title="All good",
        output_summary="summary",
        output_text="text",
        output_checks_json='[{"name":"build","status":"success","required":true}]',
        output_summary_hash="h1",
        output_text_hash="h2",
        last_eval_at="2026-02-18T12:00:01Z",
        last_publish_at="2026-02-18T12:00:02Z",
        last_publish_result="published",
        last_publish_error=None,
        last_delivery_id="d1",
    )

    settings = SimpleNamespace(
        GITHUB_APP_ID=2877723,
        PROJECTION_CHECK_RUN_NAME="merge-sentinel",
        PROJECTION_PUBLISH_ENABLED=True,
    )
    app = SimpleNamespace(
        config=settings,
        ctx=SimpleNamespace(webhook_store=store, settings=settings),
    )

    context = _state_dashboard_context(
        cast(Sanic, app),
        page=1,
        page_size=100,
        repo_filter=None,
        include_closed=True,
    )
    assert context["total"] == 1
    assert context["total_pages"] == 1
    assert len(context["rows"]) == 1
    row = context["rows"][0]
    assert row["pr_url"] == "https://github.com/org/repo/pull/42"
    assert row["pr_title"] == "Add dashboard"
    assert row["commit_url"] == f"https://github.com/org/repo/commit/{'a' * 40}"
    assert row["short_sha"] == "a" * 8
    assert row["row_key"] == "10:42"
    assert row["row_update_signature"] != ""
    assert row["pr_state_display"] == "open"
    assert row["pr_state_class"] == ""
    assert row["pr_is_draft"] is True
    assert row["pr_draft_class"] == "chip-bg-amber"
    assert (
        row["check_run_url"]
        == "https://github.com/org/repo/runs/321?check_suite_focus=true"
    )
    assert row["output_summary"] == "summary"
    assert row["output_text"] == "text"
    assert row["publish_result_display"] == "published"


def test_state_dashboard_context_can_exclude_closed_prs(tmp_path):
    store = _make_store(tmp_path / "webhooks.sqlite3")
    store.initialize()

    open_payload = {
        "action": "opened",
        "installation": {"id": 1},
        "repository": {"id": 10, "full_name": "org/repo"},
        "pull_request": {
            "id": 1001,
            "number": 1,
            "title": "Open PR",
            "state": "open",
            "updated_at": "2026-02-18T12:00:00Z",
            "head": {"sha": "a" * 40},
            "base": {"ref": "main"},
        },
    }
    closed_payload = {
        "action": "closed",
        "installation": {"id": 1},
        "repository": {"id": 10, "full_name": "org/repo"},
        "pull_request": {
            "id": 1002,
            "number": 2,
            "title": "Closed PR",
            "state": "closed",
            "merged": False,
            "updated_at": "2026-02-18T12:10:00Z",
            "head": {"sha": "b" * 40},
            "base": {"ref": "main"},
        },
    }
    store.persist_event(
        delivery_id="pr-open-1",
        event="pull_request",
        payload=open_payload,
        payload_json=store.payload_to_json(open_payload),
    )
    store.persist_event(
        delivery_id="pr-closed-1",
        event="pull_request",
        payload=closed_payload,
        payload_json=store.payload_to_json(closed_payload),
    )

    settings = SimpleNamespace(
        GITHUB_APP_ID=2877723,
        PROJECTION_CHECK_RUN_NAME="merge-sentinel",
        PROJECTION_PUBLISH_ENABLED=True,
    )
    app = SimpleNamespace(
        config=settings,
        ctx=SimpleNamespace(webhook_store=store, settings=settings),
    )

    all_rows_context = _state_dashboard_context(
        cast(Sanic, app),
        page=1,
        page_size=100,
        repo_filter=None,
        include_closed=True,
    )
    open_only_context = _state_dashboard_context(
        cast(Sanic, app),
        page=1,
        page_size=100,
        repo_filter=None,
        include_closed=False,
    )

    assert all_rows_context["total"] == 2
    assert open_only_context["total"] == 1
    assert len(open_only_context["rows"]) == 1
    assert open_only_context["rows"][0]["pr_number"] == 1
    closed_row = next(row for row in all_rows_context["rows"] if row["pr_number"] == 2)
    assert closed_row["pr_state_display"] == "closed (not merged)"
    assert closed_row["pr_state_class"] == "chip-bg-red"


@pytest.mark.asyncio
async def test_state_update_broadcaster_publish():
    broadcaster = StateUpdateBroadcaster(queue_size=2)
    queue = await broadcaster.subscribe()
    await broadcaster.publish({"source": "webhook", "event": "pull_request"})
    item = await queue.get()
    assert item["source"] == "webhook"
    assert item["event"] == "pull_request"
    await broadcaster.unsubscribe(queue)


def test_state_routes_registered():
    app = create_app()
    paths = {route.path for route in app.router.routes}
    assert "state" in paths
    assert "state/table" in paths
    assert "state/stream" in paths
    assert any("state/event" in path for path in paths)
    assert any("state/pr" in path for path in paths)
    assert any("state/pr" in path and "content" in path for path in paths)


def test_publish_result_display_for_dry_run_includes_would_post():
    assert (
        _publish_result_display(
            publish_result="dry_run",
            status="completed",
            conclusion="success",
        )
        == "completed/success (dry-run)"
    )


def test_publish_result_display_for_dry_run_draft_includes_draft_label():
    assert (
        _publish_result_display(
            publish_result="dry_run",
            status="completed",
            conclusion="success",
            is_draft=True,
        )
        == "completed/success (dry-run, draft PR)"
    )


def test_publish_result_display_for_unchanged_includes_latest_status():
    assert (
        _publish_result_display(
            publish_result="unchanged",
            status="completed",
            conclusion="success",
        )
        == "completed/success (unchanged)"
    )


def test_publish_result_display_for_unchanged_draft_includes_draft_label():
    assert (
        _publish_result_display(
            publish_result="unchanged",
            status="completed",
            conclusion="success",
            is_draft=True,
        )
        == "completed/success (unchanged, draft PR)"
    )


def test_chip_classes_for_status_and_conclusion():
    assert _status_chip_class("completed") == ""
    assert _status_chip_class("failure") == "chip-bg-red"
    assert _conclusion_chip_class("success") == "chip-bg-green"
    assert _conclusion_chip_class("failure") == "chip-bg-red"
    assert _pr_draft_chip_class(True) == "chip-bg-amber"
    assert _pr_draft_chip_class(False) == ""
    assert _pr_state_chip_class("merged") == "chip-bg-green"
    assert _pr_state_chip_class("closed") == "chip-bg-red"
    assert _pr_state_chip_class("closed (not merged)") == "chip-bg-red"
    assert _pr_state_chip_class("open") == ""


def test_state_pr_detail_context_renders_output_and_events(tmp_path):
    store = _make_store(tmp_path / "webhooks.sqlite3")
    store.initialize()

    pr_payload = {
        "action": "synchronize",
        "installation": {"id": 1},
        "repository": {"id": 10, "full_name": "org/repo"},
        "pull_request": {
            "id": 1001,
            "number": 42,
            "title": "Add dashboard",
            "draft": True,
            "state": "open",
            "updated_at": "2026-02-18T12:00:00Z",
            "head": {"sha": "a" * 40},
            "base": {"ref": "main"},
        },
    }
    store.persist_event(
        delivery_id="pr-1",
        event="pull_request",
        payload=pr_payload,
        payload_json=store.payload_to_json(pr_payload),
    )
    check_run_payload = {
        "action": "completed",
        "installation": {"id": 1},
        "repository": {"id": 10, "full_name": "org/repo"},
        "check_run": {
            "id": 333,
            "head_sha": "a" * 40,
            "name": "Builds / tests",
            "status": "completed",
            "conclusion": "success",
            "app": {"id": 99, "slug": "ci"},
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
        "id": 9001,
        "sha": "a" * 40,
        "context": "lint",
        "state": "success",
        "installation": {"id": 1},
        "repository": {"id": 10, "full_name": "org/repo"},
    }
    store.persist_event(
        delivery_id="status-1",
        event="status",
        payload=status_payload,
        payload_json=store.payload_to_json(status_payload),
    )

    store.upsert_sentinel_check_run(
        repo_id=10,
        repo_full_name="org/repo",
        check_run_id=321,
        head_sha="a" * 40,
        name="merge-sentinel",
        status="completed",
        conclusion="success",
        app_id=2877723,
        started_at="2026-02-18T11:59:00Z",
        completed_at="2026-02-18T12:00:00Z",
        output_title="All good",
        output_summary=":white_check_mark: ready",
        output_text=(
            "# Checks\n\n| Check | Status | Required? |\n| --- | --- | --- |\n"
            "| Builds / tests | success | yes |"
        ),
        output_checks_json=(
            '[{"name":"Builds / tests","status":"success","required":true,'
            '"html_url":"https://github.com/org/repo/runs/333"}]'
        ),
        output_summary_hash="h1",
        output_text_hash="h2",
        last_eval_at="2026-02-18T12:00:01Z",
        last_publish_at="2026-02-18T12:00:02Z",
        last_publish_result="dry_run",
        last_publish_error=None,
        last_delivery_id="d1",
    )

    settings = SimpleNamespace(
        GITHUB_APP_ID=2877723,
        PROJECTION_CHECK_RUN_NAME="merge-sentinel",
        PROJECTION_PUBLISH_ENABLED=True,
    )
    app = SimpleNamespace(
        config=settings,
        ctx=SimpleNamespace(webhook_store=store, settings=settings),
    )

    context = _state_pr_detail_context(cast(Sanic, app), repo_id=10, pr_number=42)
    assert context is not None
    assert context["row"]["pr_url"] == "https://github.com/org/repo/pull/42"
    assert context["row"]["pr_is_draft"] is True
    assert context["row"]["pr_draft_class"] == "chip-bg-amber"
    assert (
        context["row"]["publish_result_display"]
        == "completed/success (dry-run, draft PR)"
    )
    assert len(context["row"]["output_checks"]) == 1
    assert context["row"]["output_checks"][0]["name"] == "Builds / tests"
    assert context["row"]["output_checks"][0]["status"] == "success"
    assert context["row"]["output_checks"][0]["html_url"].endswith("/runs/333")
    assert context["row"]["output_checks"][0]["is_running"] is False
    assert "\u2705" in context["row"]["rendered_output_summary"]
    assert "rendered-check-table" in context["row"]["rendered_output_text"]
    assert len(context["events"]) >= 3
    assert context["events"][0]["delivery_id"] == "status-1"
    assert context["events"][0]["details_url"].endswith(
        "/state/event/status-1?repo_id=10&pr_number=42"
    )


def test_state_event_detail_context_renders_payload_and_back_link(tmp_path):
    store = _make_store(tmp_path / "webhooks.sqlite3")
    store.initialize()

    payload = {
        "action": "opened",
        "installation": {"id": 1},
        "repository": {"id": 10, "full_name": "org/repo"},
        "pull_request": {
            "id": 1001,
            "number": 42,
            "title": "Add dashboard",
            "state": "open",
            "head": {"sha": "a" * 40},
            "base": {"ref": "main"},
        },
    }
    store.persist_event(
        delivery_id="delivery-abc",
        event="pull_request",
        payload=payload,
        payload_json=store.payload_to_json(payload),
    )

    settings = SimpleNamespace(
        GITHUB_APP_ID=2877723,
        PROJECTION_CHECK_RUN_NAME="merge-sentinel",
        PROJECTION_PUBLISH_ENABLED=True,
    )
    app = SimpleNamespace(
        config=settings,
        ctx=SimpleNamespace(webhook_store=store, settings=settings),
    )

    context = _state_event_detail_context(
        cast(Sanic, app),
        delivery_id="delivery-abc",
        repo_id=10,
        pr_number=42,
    )
    assert context is not None
    assert context["event"]["delivery_id"] == "delivery-abc"
    assert context["event"]["event"] == "pull_request"
    assert '"number": 42' in context["event"]["payload_pretty"]
    assert context["event"]["pr_detail_url"] == "/state/pr/10/42"
