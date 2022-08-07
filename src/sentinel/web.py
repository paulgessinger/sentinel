import hmac
import logging
import logging.config
import asyncio
from pathlib import Path

from sanic import Sanic, response
import aiohttp
from gidgethub import sansio
from gidgethub.apps import get_installation_access_token, get_jwt
from gidgethub import aiohttp as gh_aiohttp

from sanic.log import logger
import sanic.log
import cachetools
import notifiers.logging

from sentinel import config
from sentinel.github import create_router
from sentinel.github.api import API
from sentinel.github.model import Repository

# logger = logging.getLogger("merge-sentinel")

# logging.basicConfig(
#     level=config.OVERRIDE_LOGGING, format="%(levelname)s %(name)s %(message)s"
# )


async def client_for_installation(app, installation_id):
    gh_pre = gh_aiohttp.GitHubAPI(app.ctx.aiohttp_session, __name__)
    access_token_response = await get_installation_access_token(
        gh_pre,
        installation_id=installation_id,
        app_id=app.config.GITHUB_APP_ID,
        private_key=app.config.GITHUB_PRIVATE_KEY,
    )

    token = access_token_response["token"]

    return gh_aiohttp.GitHubAPI(
        app.ctx.aiohttp_session,
        __name__,
        oauth_token=token,
        cache=app.ctx.cache,
    )


def create_app():

    app = Sanic("sentinel")
    app.update_config(config)
    app.config.TEMPLATING_PATH_TO_TEMPLATES = Path(__file__).parent / "templates"
    app.static("/static", Path(__file__).parent / "static")

    sanic.log.logger.setLevel(config.OVERRIDE_LOGGING)

    handler = notifiers.logging.NotificationHandler(
        "telegram",
        defaults={
            "token": config.TELEGRAM_TOKEN,
            "chat_id": config.TELEGRAM_CHAT_ID,
        },
    )
    handler.setLevel(logging.WARNING)
    handler.setFormatter(logger.handlers[0].formatter)
    logger.addHandler(handler)

    app.ctx.cache = cachetools.LRUCache(maxsize=500)
    app.ctx.github_router = create_router()

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

        app.ctx.queue_lock = asyncio.Lock()
        app.ctx.pull_request_queue = set()

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

        if event.event not in ("check_run", "pull_request"):
            return response.empty(200)

        assert "installation" in event.data
        installation_id = event.data["installation"]["id"]
        logger.debug("Installation id: %s", installation_id)

        repo = Repository.parse_obj(event.data["repository"])
        logger.debug("Repository %s", repo.full_name)

        gh = await client_for_installation(app, installation_id)

        api = API(gh)

        logger.debug("Dispatching event %s", event.event)
        try:
            await app.ctx.github_router.dispatch(event, api, app=app)
        except:
            logger.error("Exception raised when dispatching event", exc_info=True)

        return response.empty(200)

    return app
