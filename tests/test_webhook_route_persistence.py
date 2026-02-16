from types import SimpleNamespace
import sqlite3

import pytest

from sentinel.metric import error_counter
from sentinel.storage import WebhookStore
from sentinel.web import process_github_event


def make_request(delivery_id: str = "delivery-1", body: str = "{}"):
    return SimpleNamespace(
        headers={"X-GitHub-Delivery": delivery_id},
        body=body.encode("utf-8"),
    )


def make_check_run_event():
    payload = {
        "action": "completed",
        "installation": {"id": 99},
        "repository": {
            "id": 500,
            "name": "repo",
            "full_name": "org/repo",
            "url": "https://repo",
            "html_url": "https://github.com/org/repo",
            "private": False,
        },
        "check_run": {
            "id": 123,
            "head_sha": "a" * 40,
            "name": "tests",
            "status": "completed",
            "conclusion": "success",
            "started_at": "2026-02-16T10:00:00Z",
            "completed_at": "2026-02-16T10:01:00Z",
            "app": {"id": 1234, "slug": "ci"},
            "check_suite": {"id": 111},
        },
    }
    return SimpleNamespace(event="check_run", data=payload)


@pytest.mark.asyncio
async def test_supported_event_persists_without_dispatch(tmp_path):
    db_path = tmp_path / "webhooks.sqlite3"
    store = WebhookStore(str(db_path))
    store.initialize()

    app = SimpleNamespace(
        ctx=SimpleNamespace(
            webhook_store=store,
            webhook_dispatch_enabled=False,
        )
    )

    event = make_check_run_event()
    request = make_request(body=store.payload_to_json(event.data))

    await process_github_event(app, request, event)

    result = store.persist_event(
        delivery_id="delivery-1",
        event="check_run",
        payload=event.data,
        payload_json=request.body.decode(),
    )
    assert result.duplicate


@pytest.mark.asyncio
async def test_persistence_exception_is_tolerated(tmp_path, monkeypatch):
    store = WebhookStore(str(tmp_path / "webhooks.sqlite3"))
    store.initialize()

    app = SimpleNamespace(
        ctx=SimpleNamespace(
            webhook_store=store,
            webhook_dispatch_enabled=False,
        )
    )

    def fake_persist(*_args, **_kwargs):
        raise RuntimeError("db write failed")

    monkeypatch.setattr("sentinel.web.persist_webhook_event", fake_persist)

    before = error_counter.labels(context="webhook_persist")._value.get()

    await process_github_event(app, make_request(), make_check_run_event())

    after = error_counter.labels(context="webhook_persist")._value.get()
    assert after == before + 1


@pytest.mark.asyncio
async def test_unsupported_event_does_not_project(tmp_path):
    db_path = tmp_path / "webhooks.sqlite3"
    store = WebhookStore(str(db_path))
    store.initialize()

    app = SimpleNamespace(
        ctx=SimpleNamespace(
            webhook_store=store,
            webhook_dispatch_enabled=False,
        )
    )

    event = SimpleNamespace(event="issues", data={"action": "opened"})
    await process_github_event(app, make_request(), event)

    with sqlite3.connect(str(db_path)) as conn:
        count = conn.execute("SELECT COUNT(*) FROM webhook_events").fetchone()[0]
    assert count == 0


@pytest.mark.asyncio
async def test_dispatch_runs_when_enabled(tmp_path, monkeypatch):
    store = WebhookStore(str(tmp_path / "webhooks.sqlite3"))
    store.initialize()

    dispatch_called = {"value": False}

    async def fake_dispatch(_event, _api, app):
        dispatch_called["value"] = True
        assert app is not None

    app = SimpleNamespace(
        ctx=SimpleNamespace(
            webhook_store=store,
            webhook_dispatch_enabled=True,
            github_router=SimpleNamespace(dispatch=fake_dispatch),
        )
    )

    async def fake_client_for_installation(_app, _installation_id):
        return object()

    monkeypatch.setattr(
        "sentinel.web.client_for_installation", fake_client_for_installation
    )

    await process_github_event(app, make_request(), make_check_run_event())
    assert dispatch_called["value"]


@pytest.mark.asyncio
async def test_check_run_name_filter_skips_persistence(tmp_path):
    db_path = tmp_path / "webhooks.sqlite3"
    store = WebhookStore(str(db_path))
    store.initialize()

    app = SimpleNamespace(
        config=SimpleNamespace(CHECK_RUN_NAME_FILTER=r"^tests$"),
        ctx=SimpleNamespace(
            webhook_store=store,
            webhook_dispatch_enabled=False,
        ),
    )

    await process_github_event(app, make_request(), make_check_run_event())

    with sqlite3.connect(str(db_path)) as conn:
        events = conn.execute("SELECT COUNT(*) FROM webhook_events").fetchone()[0]
        checks = conn.execute("SELECT COUNT(*) FROM check_runs_current").fetchone()[0]
    assert events == 0
    assert checks == 0


@pytest.mark.asyncio
async def test_check_run_name_filter_skips_dispatch_when_enabled(tmp_path, monkeypatch):
    store = WebhookStore(str(tmp_path / "webhooks.sqlite3"))
    store.initialize()

    dispatch_called = {"value": False}

    async def fake_dispatch(_event, _api, app):
        dispatch_called["value"] = True
        assert app is not None

    app = SimpleNamespace(
        config=SimpleNamespace(CHECK_RUN_NAME_FILTER=r"^tests$"),
        ctx=SimpleNamespace(
            webhook_store=store,
            webhook_dispatch_enabled=True,
            github_router=SimpleNamespace(dispatch=fake_dispatch),
        ),
    )

    async def fake_client_for_installation(_app, _installation_id):
        return object()

    monkeypatch.setattr(
        "sentinel.web.client_for_installation", fake_client_for_installation
    )

    await process_github_event(app, make_request(), make_check_run_event())
    assert not dispatch_called["value"]
