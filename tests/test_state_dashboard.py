from types import SimpleNamespace

import pytest

from sentinel.storage import WebhookStore
from sentinel.state_dashboard import (
    StateUpdateBroadcaster,
    _state_dashboard_context,
    _state_query_params,
)
from sentinel.web import create_app


def test_state_query_params_parses_and_clamps():
    request = SimpleNamespace(args={"page": ["-2"], "page_size": ["999"], "repo": [" org/repo "]})
    page, page_size, repo = _state_query_params(request)
    assert page == 1
    assert page_size == 200
    assert repo == "org/repo"


def test_state_dashboard_context_includes_links_and_pagination(tmp_path):
    store = WebhookStore(str(tmp_path / "webhooks.sqlite3"))
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
        output_summary_hash="h1",
        output_text_hash="h2",
        last_eval_at="2026-02-18T12:00:01Z",
        last_publish_at="2026-02-18T12:00:02Z",
        last_publish_result="published",
        last_publish_error=None,
        last_delivery_id="d1",
    )

    app = SimpleNamespace(
        config=SimpleNamespace(
            GITHUB_APP_ID=2877723,
            PROJECTION_CHECK_RUN_NAME="merge-sentinel",
        ),
        ctx=SimpleNamespace(webhook_store=store),
    )

    context = _state_dashboard_context(app, page=1, page_size=100, repo_filter=None)
    assert context["total"] == 1
    assert context["total_pages"] == 1
    assert len(context["rows"]) == 1
    row = context["rows"][0]
    assert row["pr_url"] == "https://github.com/org/repo/pull/42"
    assert row["pr_title"] == "Add dashboard"
    assert row["commit_url"] == f"https://github.com/org/repo/commit/{'a'*40}"
    assert row["check_run_url"] == "https://github.com/org/repo/runs/321?check_suite_focus=true"
    assert row["output_summary"] == "summary"
    assert row["output_text"] == "text"


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
