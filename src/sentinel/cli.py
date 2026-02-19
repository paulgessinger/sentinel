import asyncio
from contextlib import asynccontextmanager
from datetime import datetime, timezone
import logging
from pathlib import Path
import shutil
import sqlite3
from typing import Annotated

import typer
from gidgethub import aiohttp as gh_aiohttp
from gidgethub.apps import get_jwt
import aiohttp
import cachetools

from sentinel.db_migrations import migrate_webhook_db as run_webhook_db_migrations
from sentinel.config import SETTINGS
from sentinel.github import get_access_token, process_pull_request, API
from sentinel.logger import get_log_handlers
from sentinel.cache import QueueItem, get_cache
from sentinel.github.model import PullRequest
from sentinel.metric import worker_error_count, record_api_call
from sentinel.storage import WebhookStore


logging.basicConfig(
    format="%(asctime)s %(name)s %(levelname)s - %(message)s", level=logging.INFO
)
logger = logging.getLogger("sentinel")

# cache = get_cache()

DEFAULT_WEBHOOK_DB_PATH = SETTINGS.WEBHOOK_DB_PATH

DEFAULT_WEBHOOK_DB_RETENTION_SECONDS = SETTINGS.WEBHOOK_DB_RETENTION_SECONDS


def _store_settings(path: Path, **updates):
    return SETTINGS.model_copy(
        update={
            "WEBHOOK_DB_PATH": path,
            **updates,
        }
    )


async def job_loop():
    logger.info("Entering job loop")
    i = 0
    while True:
        try:
            logger.debug("Sleeping for %d", SETTINGS.WORKER_SLEEP)
            await asyncio.sleep(SETTINGS.WORKER_SLEEP)

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

                if not SETTINGS.DRY_RUN:
                    async with installation_client(item.installation_id) as gh:
                        api = API(gh, item.installation_id)
                        await process_pull_request(item.pr, api)

        except KeyboardInterrupt, asyncio.exceptions.CancelledError:
            raise
        except Exception:
            worker_error_count.inc()
            logger.error("Job loop encountered error", exc_info=True)


app = typer.Typer()
httpcache = cachetools.LRUCache(maxsize=500)


@app.callback()
def init():
    logging.getLogger().setLevel(SETTINGS.OVERRIDE_LOGGING)
    logger.setLevel(SETTINGS.OVERRIDE_LOGGING)
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
            app_id=str(SETTINGS.GITHUB_APP_ID),
            private_key=SETTINGS.GITHUB_PRIVATE_KEY,
        )

        await gh.getitem("/app", jwt=jwt)
        record_api_call(endpoint="/app")

        token = await get_access_token(gh, installation)

        gh = gh_aiohttp.GitHubAPI(
            session,
            __name__,
            oauth_token=token,
            cache=httpcache,
        )

        yield gh


@app.command()
def queue_pr(
    repo: Annotated[str, typer.Argument(help="Repository in owner/name format")],
    number: Annotated[int, typer.Argument(help="Pull request number")],
    installation: Annotated[int, typer.Argument(help="GitHub App installation ID")],
):
    async def handle():
        async with installation_client(installation) as gh:
            pr = PullRequest.model_validate(
                await gh.getitem(f"/repos/{repo}/pulls/{number}")
            )
            record_api_call(endpoint=f"/repos/{repo}/pulls/{number}")

            with get_cache() as cache:
                # print("in_queue:", await cache.in_queue(pr))
                await cache.push_pr(QueueItem(pr, installation))

    asyncio.run(handle())


@app.command()
def pr(
    repo: Annotated[str, typer.Argument(help="Repository in owner/name format")],
    number: Annotated[int, typer.Argument(help="Pull request number")],
    installation: Annotated[int, typer.Argument(help="GitHub App installation ID")],
):
    async def handle():
        async with installation_client(installation) as gh:
            pr = PullRequest.model_validate(
                await gh.getitem(f"/repos/{repo}/pulls/{number}")
            )
            record_api_call(endpoint=f"/repos/{repo}/pulls/{number}")
            api = API(gh, installation)
            await process_pull_request(pr, api)

    asyncio.run(handle())


@app.command("vacuum-webhook-db")
def vacuum_webhook_db(
    path: Annotated[
        Path,
        typer.Option("--path", help="Path to webhook SQLite database file"),
    ] = DEFAULT_WEBHOOK_DB_PATH,
):
    db_path = path
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


@app.command("migrate-webhook-db")
def migrate_webhook_db(
    path: Annotated[
        Path,
        typer.Option("--path", help="Path to webhook SQLite database file"),
    ] = DEFAULT_WEBHOOK_DB_PATH,
    revision: Annotated[
        str,
        typer.Option("--revision", help="Alembic revision target (default: head)"),
    ] = "head",
):
    try:
        run_webhook_db_migrations(path, revision=revision)
    except Exception as exc:  # noqa: BLE001
        typer.echo(f"Alembic migration failed for {path}: {exc}")
        raise typer.Exit(code=1)

    typer.echo(f"Migrated webhook database to {revision}: {path}")


@app.command("prune-webhook-db")
def prune_webhook_db(
    path: Annotated[
        Path,
        typer.Option("--path", help="Path to webhook SQLite database file"),
    ] = DEFAULT_WEBHOOK_DB_PATH,
    retention_seconds: Annotated[
        int,
        typer.Option(
            "--retention-seconds",
            help="Delete webhook_events older than this many seconds",
        ),
    ] = DEFAULT_WEBHOOK_DB_RETENTION_SECONDS,
):
    db_path = path
    if not db_path.exists():
        typer.echo(f"Database does not exist: {db_path}")
        raise typer.Exit(code=1)

    if retention_seconds < 0:
        typer.echo(f"Invalid retention-seconds: {retention_seconds}")
        raise typer.Exit(code=1)

    store = WebhookStore(
        settings=_store_settings(
            db_path,
            WEBHOOK_DB_RETENTION_SECONDS=retention_seconds,
        )
    )

    try:
        pruned = store.prune_old_events()
    except sqlite3.Error as exc:
        typer.echo(f"Prune failed for {db_path}: {exc}")
        raise typer.Exit(code=1)

    typer.echo(
        f"Pruned {pruned} webhook_events older than {retention_seconds}s in {db_path}"
    )


@app.command("prune-webhook-projections")
def prune_webhook_projections(
    path: Annotated[
        Path,
        typer.Option("--path", help="Path to webhook SQLite database file"),
    ] = DEFAULT_WEBHOOK_DB_PATH,
    completed_retention_seconds: Annotated[
        int,
        typer.Option(
            "--completed-retention-seconds",
            help="Delete terminal projection rows older than this many seconds",
        ),
    ] = SETTINGS.WEBHOOK_PROJECTION_COMPLETED_RETENTION_SECONDS,
    active_retention_seconds: Annotated[
        int,
        typer.Option(
            "--active-retention-seconds",
            help="Delete active projection rows older than this many seconds",
        ),
    ] = SETTINGS.WEBHOOK_PROJECTION_ACTIVE_RETENTION_SECONDS,
):
    db_path = path
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

    store = WebhookStore(
        settings=_store_settings(
            db_path,
            WEBHOOK_PROJECTION_PRUNE_ENABLED=True,
            WEBHOOK_PROJECTION_COMPLETED_RETENTION_SECONDS=completed_retention_seconds,
            WEBHOOK_PROJECTION_ACTIVE_RETENTION_SECONDS=active_retention_seconds,
        )
    )

    try:
        counts = store.prune_old_projections()
    except sqlite3.Error as exc:
        typer.echo(f"Projection prune failed for {db_path}: {exc}")
        raise typer.Exit(code=1)

    total = sum(counts.values())
    typer.echo(
        "Pruned "
        f"{total} projection rows in {db_path} "
        f"(completed>{completed_retention_seconds}s, active>{active_retention_seconds}s)"
    )


@app.command("migrate-webhook-db-zstd")
def migrate_webhook_db_zstd(
    path: Annotated[
        Path,
        typer.Option("--path", help="Path to webhook SQLite database file"),
    ] = DEFAULT_WEBHOOK_DB_PATH,
    batch_size: Annotated[
        int,
        typer.Option(
            "--batch-size",
            help="Number of rows to update per transaction batch",
        ),
    ] = 500,
    backup: Annotated[
        bool,
        typer.Option(
            "--backup/--no-backup",
            help="Create a timestamped backup before migration",
        ),
    ] = True,
):
    db_path = path
    if not db_path.exists():
        typer.echo(f"Database does not exist: {db_path}")
        raise typer.Exit(code=1)
    if batch_size < 1:
        typer.echo(f"Invalid batch-size: {batch_size}")
        raise typer.Exit(code=1)

    if backup:
        timestamp = datetime.now(timezone.utc).strftime("%Y%m%d%H%M%S")
        backup_path = Path(f"{db_path}.bak.{timestamp}")
        try:
            shutil.copy2(db_path, backup_path)
        except OSError as exc:
            typer.echo(f"Backup failed for {db_path}: {exc}")
            raise typer.Exit(code=1)
        typer.echo(f"Backup created: {backup_path}")

    store = WebhookStore(settings=_store_settings(db_path))
    try:
        result = store.migrate_event_payloads_to_zstd(batch_size=batch_size)
    except sqlite3.Error as exc:
        typer.echo(f"Migration failed for {db_path}: {exc}")
        raise typer.Exit(code=1)

    typer.echo(
        "Migration complete for "
        f"{db_path}: scanned={result['scanned_rows']}, "
        f"converted={result['converted_rows']}, "
        f"already_compressed={result['already_compressed_rows']}, "
        f"skipped={result['skipped_rows']}"
    )
