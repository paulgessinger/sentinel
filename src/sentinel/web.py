import hmac
import logging

from sanic import Sanic, response
import aiohttp
from gidgethub import sansio
from gidgethub.apps import get_installation_access_token, get_jwt
from gidgethub import aiohttp as gh_aiohttp
from sanic.log import logger
import cachetools
import json
import asyncio

from sentinel import config
from sentinel.github import create_router
from sentinel.github.api import API


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
    logger.setLevel(config.OVERRIDE_LOGGING)

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

    @app.route("/")
    async def index(request):
        logger.debug("status check")
        return response.text("ok")

    @app.route("/webhook", methods=["POST"])
    async def github(request):
        logger.debug("Webhook received")

        event = sansio.Event.from_http(
            request.headers, request.body, secret=app.config.GITHUB_WEBHOOK_SECRET
        )

        if event.event == "ping":
            return response.empty(200)

        assert "installation" in event.data
        installation_id = event.data["installation"]["id"]
        logger.debug("Installation id: %s", installation_id)

        logger.debug("Repository %s", event.data["repository"]["full_name"])

        if config.REPO_ALLOWLIST is not None:
            if event.data["repository"]["full_name"] not in config.REPO_ALLOWLIST:
                logger.debug("Repository not in allowlist")
                return response.empty(200)

        gh = await client_for_installation(app, installation_id)

        api = API(gh)

        logger.debug("Dispatching event %s", event.event)
        await app.ctx.github_router.dispatch(event, api, app=app)

        return response.empty(200)

    return app
