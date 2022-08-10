import asyncio
from contextlib import asynccontextmanager
import logging

import typer
from gidgethub import aiohttp as gh_aiohttp
from gidgethub.apps import get_jwt, get_installation_access_token
import aiohttp
import cachetools
from sentinel.github import get_access_token, process_pull_request, API

from sentinel.logger import get_log_handlers
from sentinel import config
from sentinel.cache import QueueItem, get_cache
from sentinel.web import client_for_installation
from sentinel.github.model import PullRequest


logging.basicConfig(
    format="%(asctime)s %(name)s %(levelname)s - %(message)s", level=logging.INFO
)
logger = logging.getLogger("sentinel")

# cache = get_cache()


async def job_loop():
    logger.info("Entering job loop")
    i = 0
    while True:

        try:
            logger.debug("Sleeping for %d", config.WORKER_SLEEP)
            await asyncio.sleep(config.WORKER_SLEEP)

            with get_cache() as cache:
                if i == 60 or i == 0:
                    i = 0
                    logger.info("Queue size: %d", len(cache.deque))
                i += 1
                logger.debug("Getting item from PR queue")
                item = await cache.pull_pr()
                if item is None:
                    logger.debug("Queue empty")
                    continue

                logger.info("Processing %s", item.pr)

                if not config.DRY_RUN:
                    async with installation_client(item.installation_id) as gh:
                        api = API(gh, item.installation_id)
                        await process_pull_request(item.pr, api)

        except (KeyboardInterrupt, asyncio.exceptions.CancelledError):
            raise
        except:
            logger.error("Job loop encountered error", exc_info=True)
            pass


app = typer.Typer()
httpcache = cachetools.LRUCache(maxsize=500)


@app.callback()
def init():
    logging.getLogger().setLevel(config.OVERRIDE_LOGGING)
    logger.setLevel(config.OVERRIDE_LOGGING)
    get_log_handlers(logger)


@app.command()
def worker():
    asyncio.run(job_loop())

    # loop = asyncio.get_event_loop()

    # loop.run_until_complete(job_loop())


@asynccontextmanager
async def installation_client(installation: int):
    async with aiohttp.ClientSession(loop=asyncio.get_running_loop()) as session:
        gh = gh_aiohttp.GitHubAPI(session, __name__)

        jwt = get_jwt(
            app_id=config.GITHUB_APP_ID, private_key=config.GITHUB_PRIVATE_KEY
        )

        app_info = await gh.getitem("/app", jwt=jwt)

        token = await get_access_token(gh, installation)

        gh = gh_aiohttp.GitHubAPI(
            session,
            __name__,
            oauth_token=token,
            cache=httpcache,
        )

        yield gh


@app.command()
def queue_pr(repo: str, number: int, installation: int):
    async def handle():
        async with installation_client(installation) as gh:

            pr = PullRequest.parse_obj(
                await gh.getitem(f"/repos/{repo}/pulls/{number}")
            )

            with get_cache() as cache:
                # print("in_queue:", await cache.in_queue(pr))
                await cache.push_pr(QueueItem(pr, installation))

    asyncio.run(handle())


@app.command()
def pr(repo: str, number: int, installation: int):
    async def handle():
        async with installation_client(installation) as gh:
            pr = PullRequest.parse_obj(
                await gh.getitem(f"/repos/{repo}/pulls/{number}")
            )
            api = API(gh, installation)
            await process_pull_request(pr, api)

    asyncio.run(handle())
