from datetime import timedelta
import logging
import logging.config
from pathlib import Path
import re
import time

import aiohttp
from sanic import Sanic, response, Request
from gidgethub import sansio
from gidgethub.apps import get_jwt
from gidgethub import aiohttp as gh_aiohttp
import humanize
from sanic.log import logger
import sanic.log
from prometheus_client import core
from prometheus_client.exposition import generate_latest

from sentinel.config import SETTINGS
from sentinel.github import create_router, get_access_token
from sentinel.github.api import API
from sentinel.github.model import Repository
from sentinel.logger import get_log_handlers
from sentinel.cache import get_cache
from sentinel.metric import (
    request_counter,
    webhook_counter,
    webhook_skipped_counter,
    queue_size,
    error_counter,
    record_api_call,
    observe_view_response_latency,
    observe_webhook_processing_latency,
    configure_webhook_db_size_metric,
)
from sentinel.projection import (
    ProjectionEvaluator,
    ProjectionStatusScheduler,
    ProjectionTrigger,
)
from sentinel.state_dashboard import StateUpdateBroadcaster, register_state_routes
from sentinel.db_migrations import migrate_webhook_db as run_webhook_db_migrations
from sentinel.storage import WebhookStore


logging.basicConfig(
    format="%(asctime)s %(name)s %(levelname)s - %(message)s", level=logging.INFO
)


async def client_for_installation(app, installation_id):
    gh_pre = gh_aiohttp.GitHubAPI(app.ctx.aiohttp_session, __name__)
    token = await get_access_token(gh_pre, installation_id)

    return gh_aiohttp.GitHubAPI(
        app.ctx.aiohttp_session,
        __name__,
        oauth_token=token,
    )


def webhook_display_name(event: sansio.Event) -> str:
    if event.event == "check_run":
        return event.data["check_run"]["name"]
    if event.event == "check_suite":
        return event.data.get("action", "check_suite")
    if event.event == "workflow_run":
        workflow_run = event.data.get("workflow_run") or {}
        return workflow_run.get("name", event.data.get("action", "workflow_run"))
    if event.event == "status":
        return event.data["context"]
    if event.event == "pull_request":
        return event.data["action"]
    return "unknown"


def should_skip_event_by_name_filter(event: sansio.Event, pattern: str | None) -> bool:
    if pattern is None:
        return False
    if event.event == "check_run":
        return re.match(pattern, event.data["check_run"]["name"]) is not None
    if event.event == "status":
        return re.match(pattern, event.data["context"]) is not None
    return False


def event_source_app_id(event: sansio.Event) -> int | None:
    event_data = event.data
    if event.event == "check_run":
        app = (event_data.get("check_run") or {}).get("app") or {}
        return app.get("id")
    if event.event == "check_suite":
        app = (event_data.get("check_suite") or {}).get("app") or {}
        return app.get("id")
    if event.event == "workflow_run":
        app = (event_data.get("workflow_run") or {}).get("app") or {}
        return app.get("id")
    app = event_data.get("app") or {}
    return app.get("id")


def excluded_app_ids_for_event(app: Sanic) -> set[int]:
    settings = app.ctx.settings
    excluded = set(settings.WEBHOOK_FILTER_APP_IDS or ())
    if settings.WEBHOOK_FILTER_SELF_APP_ID:
        app_id = settings.GITHUB_APP_ID
        if app_id is not None:
            excluded.add(app_id)
    return excluded


def webhook_dispatch_enabled(app: Sanic) -> bool:
    return bool(app.ctx.settings.WEBHOOK_DISPATCH_ENABLED)


def projection_eval_enabled(app: Sanic) -> bool:
    return bool(app.ctx.settings.PROJECTION_EVAL_ENABLED)


def is_projection_manual_refresh_requested(app: Sanic, event: sansio.Event) -> bool:
    if event.event != "check_run":
        return False
    payload = event.data
    if payload.get("action") != "requested_action":
        return False

    settings = app.ctx.settings
    action_identifier = settings.PROJECTION_MANUAL_REFRESH_ACTION_IDENTIFIER
    check_run_name = settings.PROJECTION_CHECK_RUN_NAME
    github_app_id = settings.GITHUB_APP_ID

    check_run = payload.get("check_run") or {}
    requested_action = payload.get("requested_action") or {}
    if requested_action.get("identifier") != action_identifier:
        return False
    if check_run.get("name") != check_run_name:
        return False
    source_app_id = (check_run.get("app") or {}).get("id")
    return source_app_id == github_app_id


def persist_webhook_event(
    app: Sanic, event: sansio.Event, delivery_id: str, payload_json: str
):
    store: WebhookStore = app.ctx.webhook_store
    logger.debug("Should persist webhook event %s", event.event)
    if not store.should_persist(event.event):
        logger.debug("Skipping webhook event %s", event.event)
        return None

    if delivery_id == "":
        logger.warning("Missing X-GitHub-Delivery for event %s", event.event)
        return None

    logger.debug("Persisting webhook event %s", event.event)
    result = store.persist_event(
        delivery_id=delivery_id,
        event=event.event,
        payload=event.data,
        payload_json=payload_json,
    )
    if result.projection_error is not None:
        error_counter.labels(context="webhook_projection").inc()
    return result


def projection_trigger_from_event(
    event: sansio.Event,
    delivery_id: str,
    *,
    projection_check_run_name: str,
    manual_refresh_action_identifier: str,
    projection_app_id: int | None = None,
) -> ProjectionTrigger | None:
    payload = event.data
    repo = payload.get("repository") or {}
    installation = payload.get("installation") or {}
    repo_id = repo.get("id")
    repo_full_name = repo.get("full_name")
    installation_id = installation.get("id")
    if repo_id is None or repo_full_name is None or installation_id is None:
        return None

    if event.event == "check_run":
        head_sha = (payload.get("check_run") or {}).get("head_sha")
    elif event.event == "check_suite":
        head_sha = (payload.get("check_suite") or {}).get("head_sha")
    elif event.event == "workflow_run":
        head_sha = (payload.get("workflow_run") or {}).get("head_sha")
    elif event.event == "status":
        head_sha = payload.get("sha")
    elif event.event == "pull_request":
        head_sha = ((payload.get("pull_request") or {}).get("head") or {}).get("sha")
    else:
        return None

    if not head_sha:
        return None

    return ProjectionTrigger(
        repo_id=int(repo_id),
        repo_full_name=str(repo_full_name),
        head_sha=str(head_sha),
        installation_id=int(installation_id),
        delivery_id=delivery_id or None,
        event=event.event,
        action=payload.get("action"),
        force_api_refresh=bool(
            event.event == "check_run"
            and payload.get("action") == "requested_action"
            and ((payload.get("requested_action") or {}).get("identifier"))
            == manual_refresh_action_identifier
            and ((payload.get("check_run") or {}).get("name"))
            == projection_check_run_name
            and (
                projection_app_id is None
                or ((payload.get("check_run") or {}).get("app") or {}).get("id")
                == projection_app_id
            )
        ),
    )


async def process_github_event(
    app: Sanic, event: sansio.Event, delivery_id: str, payload_json: str
) -> None:
    name = webhook_display_name(event)
    webhook_counter.labels(event=event.event, name=name).inc()
    manual_refresh_requested = is_projection_manual_refresh_requested(app, event)
    source_app_id = event_source_app_id(event)
    excluded_app_ids = excluded_app_ids_for_event(app)

    if (
        not manual_refresh_requested
        and source_app_id is not None
        and source_app_id in excluded_app_ids
    ):
        webhook_skipped_counter.labels(event=event.event, name=name).inc()
        logger.debug(
            "Skipping webhook event %s name=%s due to source app id=%s",
            event.event,
            name,
            source_app_id,
        )
        return

    settings = app.ctx.settings
    name_filter = settings.CHECK_RUN_NAME_FILTER

    if (not manual_refresh_requested) and should_skip_event_by_name_filter(
        event, name_filter
    ):
        webhook_skipped_counter.labels(event=event.event, name=name).inc()
        logger.debug(
            "Skipping webhook event %s name=%s due to CHECK_RUN_NAME_FILTER=%s",
            event.event,
            name,
            name_filter,
        )
        return

    persist_result = None
    if app.ctx.settings.WEBHOOK_DB_ENABLED:
        try:
            persist_result = persist_webhook_event(
                app, event, delivery_id, payload_json
            )
        except Exception:  # noqa: BLE001
            error_counter.labels(context="webhook_persist").inc()
            logger.error(
                "Exception raised when persisting webhook event %s",
                event.event,
                exc_info=True,
            )
    if (
        persist_result is not None
        and persist_result.inserted
        and persist_result.projection_error is None
    ):
        try:
            await app.ctx.state_broadcaster.publish(
                {
                    "source": "webhook",
                    "event": event.event,
                    "delivery_id": delivery_id,
                }
            )
        except Exception:  # noqa: BLE001
            logger.debug(
                "Failed to publish state update for webhook event", exc_info=True
            )

    if (
        projection_eval_enabled(app)
        and persist_result is not None
        and persist_result.inserted
        and persist_result.projection_error is None
        and app.ctx.projection_scheduler is not None
    ):
        trigger = projection_trigger_from_event(
            event,
            delivery_id,
            projection_check_run_name=settings.PROJECTION_CHECK_RUN_NAME,
            manual_refresh_action_identifier=(
                settings.PROJECTION_MANUAL_REFRESH_ACTION_IDENTIFIER
            ),
            projection_app_id=settings.GITHUB_APP_ID,
        )
        if trigger is not None:
            try:
                await app.ctx.projection_scheduler.enqueue(trigger)
            except Exception:  # noqa: BLE001
                error_counter.labels(context="projection_schedule").inc()
                logger.error(
                    "Failed scheduling projection evaluation for repo_id=%s sha=%s",
                    trigger.repo_id,
                    trigger.head_sha,
                    exc_info=True,
                )

    if not webhook_dispatch_enabled(app):
        return

    if event.event not in ("check_run", "status", "pull_request"):
        return

    assert "installation" in event.data
    installation_id = event.data["installation"]["id"]
    logger.debug("Installation id: %s", installation_id)

    repo = Repository.model_validate(event.data["repository"])
    logger.debug("Repository %s", repo.full_name)

    gh = await client_for_installation(app, installation_id)
    api = API(gh, installation_id)

    logger.debug("Dispatching event %s", event.event)
    try:
        await app.ctx.github_router.dispatch(event, api, app=app)
    except Exception:  # noqa: BLE001
        error_counter.labels(context="event_dispatch").inc()
        logger.error("Exception raised when dispatching event", exc_info=True)


async def process_github_event_background(
    app: Sanic, event: sansio.Event, delivery_id: str, payload_json: str
) -> None:
    started_at = time.perf_counter()
    result = "ok"
    try:
        await process_github_event(app, event, delivery_id, payload_json)
    except Exception:  # noqa: BLE001
        result = "error"
        error_counter.labels(context="event_background").inc()
        logger.error(
            "Unhandled exception in background webhook processing for event %s",
            event.event,
            exc_info=True,
        )
    finally:
        observe_webhook_processing_latency(
            event=event.event,
            result=result,
            seconds=time.perf_counter() - started_at,
        )


def create_app():

    app = Sanic("sentinel")
    app.ctx.settings = SETTINGS
    app.config.TEMPLATING_PATH_TO_TEMPLATES = Path(__file__).parent / "templates"
    app.static("/static", Path(__file__).parent / "static")

    # print(SETTINGS.OVERRIDE_LOGGING, logging.DEBUG)
    logging.getLogger().setLevel(SETTINGS.OVERRIDE_LOGGING)
    # sanic.log.logger.setLevel(logging.INFO)

    sanic.log.logger.handlers = []

    for handler in get_log_handlers(sanic.log.logger):
        handler.setFormatter(logger.handlers[0].formatter)

    app.ctx.github_router = create_router()
    app.ctx.projection_scheduler = None
    app.ctx.state_broadcaster = StateUpdateBroadcaster()
    app.ctx.webhook_store = WebhookStore(settings=app.ctx.settings)

    # app.register_middleware(make_asgi_app())

    @app.listener("before_server_start")
    async def init(app):
        settings = app.ctx.settings
        configure_webhook_db_size_metric(settings.WEBHOOK_DB_PATH)

        if webhook_dispatch_enabled(app) or projection_eval_enabled(app):
            logger.debug("Creating aiohttp session")
            app.ctx.aiohttp_session = aiohttp.ClientSession()

            if webhook_dispatch_enabled(app):
                gh = gh_aiohttp.GitHubAPI(app.ctx.aiohttp_session, __name__)
                jwt = get_jwt(
                    app_id=str(settings.GITHUB_APP_ID),
                    private_key=settings.GITHUB_PRIVATE_KEY,
                )
                app.ctx.app_info = await gh.getitem("/app", jwt=jwt)
                record_api_call(endpoint="/app")

        if app.ctx.settings.WEBHOOK_DB_ENABLED:
            logger.info(
                "Running webhook DB migrations to head for %s",
                settings.WEBHOOK_DB_PATH,
            )
            run_webhook_db_migrations(settings.WEBHOOK_DB_PATH, revision="head")

        app.ctx.webhook_store.initialize()
        app.ctx.webhook_store.prune_old_events()
        app.ctx.webhook_store.prune_old_activity_events()
        if settings.WEBHOOK_PROJECTION_PRUNE_ENABLED:
            app.ctx.webhook_store.prune_old_projections()

        if projection_eval_enabled(app):

            async def projection_api_factory(installation_id: int) -> API:
                gh = await client_for_installation(app, installation_id)
                return API(gh, installation_id)

            evaluator = ProjectionEvaluator(
                store=app.ctx.webhook_store,
                config=settings,
                api_factory=projection_api_factory,
            )

            async def projection_handler(trigger: ProjectionTrigger) -> None:
                result = await evaluator.evaluate_and_publish(trigger)
                try:
                    await app.ctx.state_broadcaster.publish(
                        {
                            "source": "projection",
                            "event": trigger.event,
                            "delivery_id": trigger.delivery_id,
                            "repo_id": trigger.repo_id,
                            "head_sha": trigger.head_sha,
                            "result": result.result,
                        }
                    )
                except Exception:  # noqa: BLE001
                    logger.debug(
                        "Failed to publish state update for projection evaluation",
                        exc_info=True,
                    )

            app.ctx.projection_scheduler = ProjectionStatusScheduler(
                debounce_seconds=settings.PROJECTION_DEBOUNCE_SECONDS,
                pull_request_synchronize_delay_seconds=(
                    settings.PROJECTION_PULL_REQUEST_SYNCHRONIZE_DELAY_SECONDS
                ),
                handler=projection_handler,
            )

    @app.listener("after_server_stop")
    async def shutdown(app):
        scheduler = getattr(app.ctx, "projection_scheduler", None)
        if scheduler is not None:
            await scheduler.shutdown()
        session = getattr(app.ctx, "aiohttp_session", None)
        if session is not None and not session.closed:
            await session.close()

    @app.on_request
    async def on_request(request: Request):
        request.ctx.request_started_at = time.perf_counter()
        if request.path == "/metrics":
            return
        request_counter.labels(path=request.path).inc()

    @app.on_response
    async def on_response(request: Request, response):
        started_at = getattr(request.ctx, "request_started_at", None)
        if started_at is None:
            return
        if request.path == "/metrics":
            return
        elapsed_seconds = time.perf_counter() - float(started_at)
        route = getattr(request, "route", None)
        path_label = route.path if route is not None else request.path
        if request.path == "/webhook":
            observe_view_response_latency(
                path=str(path_label),
                method=request.method,
                status=getattr(response, "status", 0),
                seconds=elapsed_seconds,
            )
            return
        content_type = (getattr(response, "content_type", None) or "").lower()
        if not content_type.startswith("text/html"):
            return

        observe_view_response_latency(
            path=str(path_label),
            method=request.method,
            status=getattr(response, "status", 0),
            seconds=elapsed_seconds,
        )

    @app.get("/")
    @app.ext.template("index.html.j2")
    async def index(request):
        if SETTINGS.INDEX_REDIRECT_URL:
            return response.redirect(SETTINGS.INDEX_REDIRECT_URL)
        return {"app": request.app}

    @app.get("/status")
    async def status(request):
        logger.debug("status check")
        return response.text("ok")

    @app.route("/webhook", methods=["POST"])
    async def github(request):
        logger.debug("Webhook received")
        settings = app.ctx.settings

        event = sansio.Event.from_http(
            request.headers, request.body, secret=settings.GITHUB_WEBHOOK_SECRET
        )
        delivery_id = request.headers.get("X-GitHub-Delivery", "")
        payload_json = request.body.decode("utf-8", errors="replace")

        app.add_task(
            process_github_event_background(app, event, delivery_id, payload_json)
        )

        return response.empty(200)

    register_state_routes(app)

    def _queue_rows():
        with get_cache() as dcache:
            cooldown = timedelta(seconds=SETTINGS.PR_TIMEOUT)
            data = []
            for item in dcache.deque:
                delta = None
                last_dt = dcache.get(f"{dcache.pr_cooldown_key}_{item.pr.id}")
                data.append(
                    (
                        item.pr,
                        (
                            humanize.naturaltime(last_dt + cooldown)
                            if last_dt is not None
                            else None
                            if delta is not None
                            else None
                        ),
                    )
                )
            return data

    @app.get("/queue")
    @app.ext.template("queue.html.j2")
    async def queue(request):
        return {"app": request.app, "prs": _queue_rows()}

    @app.get("/queue/inner")
    @app.ext.template("queue_inner.html.j2")
    async def queue_inner(request):
        return {"app": request.app, "prs": _queue_rows()}

    @app.get("/metrics")
    async def metrics(request):
        with get_cache() as dcache:
            async with dcache.lock:
                queue_size.set(len(dcache.deque))
                # api_call_count._value.set(dcache.get("num_api_requests"))

        # if not self._multiprocess_on:
        registry = core.REGISTRY
        # else:
        #     registry = CollectorRegistry()
        #     multiprocess.MultiProcessCollector(registry)
        data = generate_latest(registry)
        # print(data)
        return response.text(data.decode("utf-8"))

    # @app.route("/test/<repo>/<installation_id>/<number>")
    # async def test(request, repo: str, installation_id: int, number: int):
    #     async with installation_client(installation_id) as gh:

    #         pr = PullRequest.model_validate(
    #             await gh.getitem(f"/repos/{repo.replace('__', '/')}/pulls/{number}")
    #         )

    #         with get_cache() as cache:
    #             # print("in_queue:", await cache.in_queue(pr))
    #             await cache.push_pr(QueueItem(pr, installation_id))
    #     return response.empty(200)

    return app
