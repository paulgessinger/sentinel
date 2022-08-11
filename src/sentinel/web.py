from datetime import datetime, timedelta
import hmac
import json
import logging
import logging.config
import asyncio
from pathlib import Path

from sanic import Sanic, response, Request
import aiohttp
from gidgethub import sansio
from gidgethub.apps import get_installation_access_token, get_jwt
from gidgethub import aiohttp as gh_aiohttp
import humanize
from sanic.log import logger
import sanic.log
import cachetools
import notifiers.logging
from prometheus_client import core
from prometheus_client.exposition import generate_latest

from sentinel import config
from sentinel.github import create_router, get_access_token, process_pull_request
from sentinel.github.api import API
from sentinel.github.model import PullRequest, Repository
from sentinel.logger import get_log_handlers
from sentinel.cache import Cache, QueueItem, get_cache
from sentinel.metric import (
    request_counter,
    webhook_counter,
    queue_size,
    error_counter,
    api_call_count,
)


async def client_for_installation(app, installation_id):
    gh_pre = gh_aiohttp.GitHubAPI(app.ctx.aiohttp_session, __name__)
    # access_token_response = await get_installation_access_token(
    #     gh_pre,
    #     installation_id=installation_id,
    #     app_id=app.config.GITHUB_APP_ID,
    #     private_key=app.config.GITHUB_PRIVATE_KEY,
    # )

    # token = access_token_response["token"]
    token = await get_access_token(gh_pre, installation_id)

    return gh_aiohttp.GitHubAPI(
        app.ctx.aiohttp_session,
        __name__,
        oauth_token=token,
        cache=app.ctx.cache,
    )


logging.basicConfig(
    format="%(asctime)s %(name)s %(levelname)s - %(message)s", level=logging.INFO
)


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

    app.ctx.cache = cachetools.LRUCache(maxsize=500)
    app.ctx.github_router = create_router()

    # app.register_middleware(make_asgi_app())

    @app.listener("before_server_start")
    async def init(app, loop):
        logger.debug("Creating aiohttp session")
        app.ctx.aiohttp_session = aiohttp.ClientSession(loop=loop)

        gh = gh_aiohttp.GitHubAPI(app.ctx.aiohttp_session, __name__)

        jwt = get_jwt(
            app_id=app.config.GITHUB_APP_ID, private_key=app.config.GITHUB_PRIVATE_KEY
        )
        app_info = await gh.getitem("/app", jwt=jwt)
        app.ctx.app_info = app_info

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

        webhook_counter.labels(event=event.event).inc()

        if event.event not in ("check_run", "status", "pull_request"):
            return response.empty(200)

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
        except:
            error_counter.labels(context="event_dispatch").inc()
            logger.error("Exception raised when dispatching event", exc_info=True)

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
                        humanize.naturaltime(last_dt + cooldown)
                        if last_dt is not None
                        else None
                        if delta is not None
                        else None,
                    )
                )

            print(request.args.get("inner"))
            return {"app": app, "prs": data, "inner": request.args.get("inner")}

    @app.get("/metrics")
    async def metrics(request):
        with get_cache() as dcache:
            async with dcache.lock:
                queue_size.set(len(dcache.deque))
                api_call_count._value.set(dcache.get("num_api_requests"))

        # if not self._multiprocess_on:
        registry = core.REGISTRY
        # else:
        #     registry = CollectorRegistry()
        #     multiprocess.MultiProcessCollector(registry)
        data = generate_latest(registry)
        print(data)
        return response.raw(data)

    # @app.route("/test/<repo>/<installation_id>/<number>")
    # async def test(request, repo: str, installation_id: int, number: int):
    #     async with installation_client(installation_id) as gh:

    #         pr = PullRequest.parse_obj(
    #             await gh.getitem(f"/repos/{repo.replace('__', '/')}/pulls/{number}")
    #         )

    #         with get_cache() as cache:
    #             # print("in_queue:", await cache.in_queue(pr))
    #             await cache.push_pr(QueueItem(pr, installation_id))
    #     return response.empty(200)

    return app
