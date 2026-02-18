from types import SimpleNamespace
import sqlite3

import pytest

from sentinel.metric import error_counter
from sentinel.storage import WebhookStore
from sentinel.web import process_github_event


class _RecordingBroadcaster:
    def __init__(self):
        self.events = []

    async def publish(self, payload):
        self.events.append(payload)


def make_request(delivery_id: str = "delivery-1", body: str = "{}"):
    return SimpleNamespace(
        headers={"X-GitHub-Delivery": delivery_id},
        body=body.encode("utf-8"),
    )


def make_check_run_event(app_id: int = 1234):
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
            "app": {"id": app_id, "slug": "ci"},
            "check_suite": {"id": 111},
        },
    }
    return SimpleNamespace(event="check_run", data=payload)


def make_config(**overrides):
    data = {
        "WEBHOOK_DISPATCH_ENABLED": False,
        "PROJECTION_EVAL_ENABLED": False,
    }
    data.update(overrides)
    return SimpleNamespace(**data)


@pytest.mark.asyncio
async def test_supported_event_persists_without_dispatch(tmp_path):
    db_path = tmp_path / "webhooks.sqlite3"
    store = WebhookStore(str(db_path))
    store.initialize()

    broadcaster = _RecordingBroadcaster()
    app = SimpleNamespace(
        config=make_config(),
        ctx=SimpleNamespace(
            webhook_store=store,
            state_broadcaster=broadcaster,
        )
    )

    event = make_check_run_event()
    request = make_request(body=store.payload_to_json(event.data))

    await process_github_event(
        app,
        event,
        request.headers["X-GitHub-Delivery"],
        request.body.decode("utf-8"),
    )

    result = store.persist_event(
        delivery_id="delivery-1",
        event="check_run",
        payload=event.data,
        payload_json=request.body.decode(),
    )
    assert result.duplicate
    assert len(broadcaster.events) == 1
    assert broadcaster.events[0]["source"] == "webhook"


@pytest.mark.asyncio
async def test_supported_event_enqueues_projection_eval_when_enabled(tmp_path):
    db_path = tmp_path / "webhooks.sqlite3"
    store = WebhookStore(str(db_path))
    store.initialize()

    enqueued = []

    class _Scheduler:
        async def enqueue(self, trigger):
            enqueued.append(trigger)

    broadcaster = _RecordingBroadcaster()
    app = SimpleNamespace(
        config=make_config(PROJECTION_EVAL_ENABLED=True),
        ctx=SimpleNamespace(
            webhook_store=store,
            projection_scheduler=_Scheduler(),
            state_broadcaster=broadcaster,
        )
    )

    request = make_request()
    event = make_check_run_event()

    await process_github_event(
        app,
        event,
        request.headers["X-GitHub-Delivery"],
        request.body.decode("utf-8"),
    )

    assert len(enqueued) == 1
    assert enqueued[0].repo_id == 500
    assert enqueued[0].head_sha == "a" * 40
    assert enqueued[0].action == "completed"


@pytest.mark.asyncio
async def test_persistence_exception_is_tolerated(tmp_path, monkeypatch):
    store = WebhookStore(str(tmp_path / "webhooks.sqlite3"))
    store.initialize()

    app = SimpleNamespace(
        config=make_config(),
        ctx=SimpleNamespace(
            webhook_store=store,
            state_broadcaster=_RecordingBroadcaster(),
        )
    )

    def fake_persist(*_args, **_kwargs):
        raise RuntimeError("db write failed")

    monkeypatch.setattr("sentinel.web.persist_webhook_event", fake_persist)

    before = error_counter.labels(context="webhook_persist")._value.get()

    request = make_request()
    event = make_check_run_event()
    await process_github_event(
        app,
        event,
        request.headers["X-GitHub-Delivery"],
        request.body.decode("utf-8"),
    )

    after = error_counter.labels(context="webhook_persist")._value.get()
    assert after == before + 1


@pytest.mark.asyncio
async def test_unsupported_event_does_not_project(tmp_path):
    db_path = tmp_path / "webhooks.sqlite3"
    store = WebhookStore(str(db_path))
    store.initialize()

    app = SimpleNamespace(
        config=make_config(),
        ctx=SimpleNamespace(
            webhook_store=store,
            state_broadcaster=_RecordingBroadcaster(),
        )
    )

    event = SimpleNamespace(event="issues", data={"action": "opened"})
    request = make_request()
    await process_github_event(
        app,
        event,
        request.headers["X-GitHub-Delivery"],
        request.body.decode("utf-8"),
    )

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
        config=make_config(WEBHOOK_DISPATCH_ENABLED=True),
        ctx=SimpleNamespace(
            webhook_store=store,
            github_router=SimpleNamespace(dispatch=fake_dispatch),
            state_broadcaster=_RecordingBroadcaster(),
        )
    )

    async def fake_client_for_installation(_app, _installation_id):
        return object()

    monkeypatch.setattr(
        "sentinel.web.client_for_installation", fake_client_for_installation
    )

    request = make_request()
    event = make_check_run_event()
    await process_github_event(
        app,
        event,
        request.headers["X-GitHub-Delivery"],
        request.body.decode("utf-8"),
    )
    assert dispatch_called["value"]


@pytest.mark.asyncio
async def test_check_run_name_filter_skips_persistence(tmp_path):
    db_path = tmp_path / "webhooks.sqlite3"
    store = WebhookStore(str(db_path))
    store.initialize()

    app = SimpleNamespace(
        config=make_config(CHECK_RUN_NAME_FILTER=r"^tests$"),
        ctx=SimpleNamespace(
            webhook_store=store,
            state_broadcaster=_RecordingBroadcaster(),
        ),
    )

    request = make_request()
    event = make_check_run_event()
    await process_github_event(
        app,
        event,
        request.headers["X-GitHub-Delivery"],
        request.body.decode("utf-8"),
    )

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
        config=make_config(
            CHECK_RUN_NAME_FILTER=r"^tests$",
            WEBHOOK_DISPATCH_ENABLED=True,
        ),
        ctx=SimpleNamespace(
            webhook_store=store,
            github_router=SimpleNamespace(dispatch=fake_dispatch),
            state_broadcaster=_RecordingBroadcaster(),
        ),
    )

    async def fake_client_for_installation(_app, _installation_id):
        return object()

    monkeypatch.setattr(
        "sentinel.web.client_for_installation", fake_client_for_installation
    )

    request = make_request()
    event = make_check_run_event()
    await process_github_event(
        app,
        event,
        request.headers["X-GitHub-Delivery"],
        request.body.decode("utf-8"),
    )
    assert not dispatch_called["value"]


@pytest.mark.asyncio
async def test_self_app_id_filter_skips_persistence(tmp_path):
    db_path = tmp_path / "webhooks.sqlite3"
    store = WebhookStore(str(db_path))
    store.initialize()

    app = SimpleNamespace(
        config=make_config(
            GITHUB_APP_ID=1234,
            WEBHOOK_FILTER_SELF_APP_ID=True,
            WEBHOOK_FILTER_APP_IDS=(),
        ),
        ctx=SimpleNamespace(
            webhook_store=store,
            state_broadcaster=_RecordingBroadcaster(),
        ),
    )

    request = make_request()
    event = make_check_run_event(app_id=1234)
    await process_github_event(
        app,
        event,
        request.headers["X-GitHub-Delivery"],
        request.body.decode("utf-8"),
    )

    with sqlite3.connect(str(db_path)) as conn:
        count = conn.execute("SELECT COUNT(*) FROM webhook_events").fetchone()[0]
    assert count == 0


@pytest.mark.asyncio
async def test_configured_app_id_filter_skips_persistence(tmp_path):
    db_path = tmp_path / "webhooks.sqlite3"
    store = WebhookStore(str(db_path))
    store.initialize()

    app = SimpleNamespace(
        config=make_config(
            GITHUB_APP_ID=1234,
            WEBHOOK_FILTER_SELF_APP_ID=False,
            WEBHOOK_FILTER_APP_IDS=(8888, 9999),
        ),
        ctx=SimpleNamespace(
            webhook_store=store,
            state_broadcaster=_RecordingBroadcaster(),
        ),
    )

    request = make_request()
    event = make_check_run_event(app_id=8888)
    await process_github_event(
        app,
        event,
        request.headers["X-GitHub-Delivery"],
        request.body.decode("utf-8"),
    )

    with sqlite3.connect(str(db_path)) as conn:
        count = conn.execute("SELECT COUNT(*) FROM webhook_events").fetchone()[0]
    assert count == 0
