from datetime import datetime, timedelta
import logging
import logging.config
from pathlib import Path
import re

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

from sentinel import config
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
)
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


def persist_webhook_event(app: Sanic, request: Request, event: sansio.Event):
    store: WebhookStore = app.ctx.webhook_store
    logger.debug("Should persist webhook event %s", event.event)
    if not store.should_persist(event.event):
        logger.debug("Skipping webhook event %s", event.event)
        return None

    delivery_id = request.headers.get("X-GitHub-Delivery", "")
    if delivery_id == "":
        logger.warning("Missing X-GitHub-Delivery for event %s", event.event)
        return None

    payload_json = request.body.decode("utf-8", errors="replace")

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


async def process_github_event(
    app: Sanic, request: Request, event: sansio.Event
) -> None:
    name = webhook_display_name(event)
    webhook_counter.labels(event=event.event, name=name).inc()
    name_filter = getattr(getattr(app, "config", None), "CHECK_RUN_NAME_FILTER", None)

    if should_skip_event_by_name_filter(event, name_filter):
        webhook_skipped_counter.labels(event=event.event, name=name).inc()
        logger.debug(
            "Skipping webhook event %s name=%s due to CHECK_RUN_NAME_FILTER=%s",
            event.event,
            name,
            name_filter,
        )
        return

    if app.ctx.webhook_store.enabled:
        try:
            persist_webhook_event(app, request, event)
        except Exception:  # noqa: BLE001
            error_counter.labels(context="webhook_persist").inc()
            logger.error(
                "Exception raised when persisting webhook event %s",
                event.event,
                exc_info=True,
            )

    if not getattr(app.ctx, "webhook_dispatch_enabled", False):
        return

    if event.event not in ("check_run", "status", "pull_request"):
        return

    assert "installation" in event.data
    installation_id = event.data["installation"]["id"]
    logger.debug("Installation id: %s", installation_id)

    repo = Repository.parse_obj(event.data["repository"])
    logger.debug("Repository %s", repo.full_name)

    gh = await client_for_installation(app, installation_id)
    api = API(gh, installation_id)

    logger.debug("Dispatching event %s", event.event)
    try:
        await app.ctx.github_router.dispatch(event, api, app=app)
    except Exception:  # noqa: BLE001
        error_counter.labels(context="event_dispatch").inc()
        logger.error("Exception raised when dispatching event", exc_info=True)


def create_app():

    app = Sanic("sentinel")
    app.update_config(config)
    app.config.TEMPLATING_PATH_TO_TEMPLATES = Path(__file__).parent / "templates"
    app.static("/static", Path(__file__).parent / "static")

    # print(config.OVERRIDE_LOGGING, logging.DEBUG)
    logging.getLogger().setLevel(config.OVERRIDE_LOGGING)
    # sanic.log.logger.setLevel(logging.INFO)

    sanic.log.logger.handlers = []

    for handler in get_log_handlers(sanic.log.logger):
        handler.setFormatter(logger.handlers[0].formatter)

    app.ctx.github_router = create_router()
    app.ctx.webhook_dispatch_enabled = app.config.WEBHOOK_DISPATCH_ENABLED
    app.ctx.webhook_store = WebhookStore(
        db_path=app.config.WEBHOOK_DB_PATH,
        retention_days=app.config.WEBHOOK_DB_RETENTION_DAYS,
        enabled=app.config.WEBHOOK_DB_ENABLED,
        events=app.config.WEBHOOK_DB_EVENTS,
    )

    # app.register_middleware(make_asgi_app())

    @app.listener("before_server_start")
    async def init(app):
        if app.ctx.webhook_dispatch_enabled:
            logger.debug("Creating aiohttp session")
            app.ctx.aiohttp_session = aiohttp.ClientSession()

            gh = gh_aiohttp.GitHubAPI(app.ctx.aiohttp_session, __name__)
            jwt = get_jwt(
                app_id=str(app.config.GITHUB_APP_ID),
                private_key=app.config.GITHUB_PRIVATE_KEY,
            )
            app.ctx.app_info = await gh.getitem("/app", jwt=jwt)

        app.ctx.webhook_store.initialize()
        app.ctx.webhook_store.prune_old_events()

    @app.listener("after_server_stop")
    async def shutdown(app):
        session = getattr(app.ctx, "aiohttp_session", None)
        if session is not None and not session.closed:
            await session.close()

    @app.on_request
    async def on_request(request: Request):
        if request.path == "/metrics":
            return
        request_counter.labels(path=request.path).inc()

    @app.get("/")
    @app.ext.template("index.html.j2")
    async def index(request):
        return {"app": app}

    @app.get("/status")
    async def status(request):
        logger.debug("status check")
        return response.text("ok")

    @app.route("/webhook", methods=["POST"])
    async def github(request):
        logger.debug("Webhook received")

        event = sansio.Event.from_http(
            request.headers, request.body, secret=app.config.GITHUB_WEBHOOK_SECRET
        )

        await process_github_event(app, request, event)

        return response.empty(200)

    @app.route("/queue")
    @app.ext.template("queue.html.j2")
    async def queue(request):
        with get_cache() as dcache:

            cooldown = timedelta(seconds=config.PR_TIMEOUT)
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
                            else None if delta is not None else None
                        ),
                    )
                )

            print(request.args.get("inner"))
            return {"app": app, "prs": data, "inner": request.args.get("inner")}

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
