import asyncio
from contextlib import asynccontextmanager
import logging
from pathlib import Path
import sqlite3

import typer
from gidgethub import aiohttp as gh_aiohttp
from gidgethub.apps import get_jwt, get_installation_access_token
import aiohttp
import cachetools
from prometheus_client import push_to_gateway

from sentinel.github import get_access_token, process_pull_request, API
from sentinel.logger import get_log_handlers
from sentinel import config
from sentinel.cache import Cache, QueueItem, get_cache
from sentinel.github.model import PullRequest
from sentinel.metric import push_registry, worker_error_count, api_call_count
from sentinel.storage import WebhookStore


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

                api_call_count.inc(api.call_count)

        except (KeyboardInterrupt, asyncio.exceptions.CancelledError):
            raise
        except:
            worker_error_count.inc()
            logger.error("Job loop encountered error", exc_info=True)
            pass
        finally:
            if config.PUSH_GATEWAY is not None:
                try:
                    push_to_gateway(
                        config.PUSH_GATEWAY, "sentinel_worker", push_registry
                    )
                except:
                    logger.error(
                        "Error pushing to pushgateway %s",
                        config.PUSH_GATEWAY,
                        exc_info=True,
                    )
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
            app_id=str(config.GITHUB_APP_ID),
            private_key=config.GITHUB_PRIVATE_KEY,
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

            pr = PullRequest.model_validate(
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
            pr = PullRequest.model_validate(
                await gh.getitem(f"/repos/{repo}/pulls/{number}")
            )
            api = API(gh, installation)
            await process_pull_request(pr, api)

    asyncio.run(handle())


@app.command("vacuum-webhook-db")
def vacuum_webhook_db(
    path: str = typer.Option(
        config.WEBHOOK_DB_PATH,
        "--path",
        help="Path to webhook SQLite database file",
    ),
):
    db_path = Path(path)
    if not db_path.exists():
        typer.echo(f"Database does not exist: {db_path}")
        raise typer.Exit(code=1)

    try:
        with sqlite3.connect(str(db_path), timeout=60.0) as conn:
            conn.execute("PRAGMA busy_timeout=10000")
            conn.execute("VACUUM")
    except sqlite3.Error as exc:
        typer.echo(f"VACUUM failed for {db_path}: {exc}")
        raise typer.Exit(code=1)

    typer.echo(f"VACUUM completed for {db_path}")


@app.command("prune-webhook-db")
def prune_webhook_db(
    path: str = typer.Option(
        config.WEBHOOK_DB_PATH,
        "--path",
        help="Path to webhook SQLite database file",
    ),
    retention_seconds: int = typer.Option(
        config.WEBHOOK_DB_RETENTION_SECONDS,
        "--retention-seconds",
        help="Delete webhook_events older than this many seconds",
    ),
):
    db_path = Path(path)
    if not db_path.exists():
        typer.echo(f"Database does not exist: {db_path}")
        raise typer.Exit(code=1)

    if retention_seconds < 0:
        typer.echo(f"Invalid retention-seconds: {retention_seconds}")
        raise typer.Exit(code=1)

    store = WebhookStore(
        db_path=str(db_path),
        retention_seconds=retention_seconds,
    )

    try:
        pruned = store.prune_old_events(retention_seconds=retention_seconds)
    except sqlite3.Error as exc:
        typer.echo(f"Prune failed for {db_path}: {exc}")
        raise typer.Exit(code=1)

    typer.echo(
        f"Pruned {pruned} webhook_events older than {retention_seconds}s in {db_path}"
    )


@app.command("prune-webhook-projections")
def prune_webhook_projections(
    path: str = typer.Option(
        config.WEBHOOK_DB_PATH,
        "--path",
        help="Path to webhook SQLite database file",
    ),
    completed_retention_seconds: int = typer.Option(
        config.WEBHOOK_PROJECTION_COMPLETED_RETENTION_SECONDS,
        "--completed-retention-seconds",
        help="Delete terminal projection rows older than this many seconds",
    ),
    active_retention_seconds: int = typer.Option(
        config.WEBHOOK_PROJECTION_ACTIVE_RETENTION_SECONDS,
        "--active-retention-seconds",
        help="Delete active projection rows older than this many seconds",
    ),
):
    db_path = Path(path)
    if not db_path.exists():
        typer.echo(f"Database does not exist: {db_path}")
        raise typer.Exit(code=1)

    if completed_retention_seconds < 0:
        typer.echo(
            f"Invalid completed-retention-seconds: {completed_retention_seconds}"
        )
        raise typer.Exit(code=1)
    if active_retention_seconds < 0:
        typer.echo(f"Invalid active-retention-seconds: {active_retention_seconds}")
        raise typer.Exit(code=1)

    store = WebhookStore(db_path=str(db_path))

    try:
        counts = store.prune_old_projections(
            completed_retention_seconds=completed_retention_seconds,
            active_retention_seconds=active_retention_seconds,
        )
    except sqlite3.Error as exc:
        typer.echo(f"Projection prune failed for {db_path}: {exc}")
        raise typer.Exit(code=1)

    total = sum(counts.values())
    typer.echo(
        "Pruned "
        f"{total} projection rows in {db_path} "
        f"(completed>{completed_retention_seconds}s, active>{active_retention_seconds}s)"
    )
