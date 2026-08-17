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
from sentinel.projection import ProjectionEvaluator, ProjectionTrigger
from sentinel.storage import WebhookStore
from sentinel.storage.types import PullRequestHeadRow


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


@asynccontextmanager
async def installation_client_for_repo(repo: str):
    """Like `installation_client`, but resolves the installation ID from a repo
    via the GitHub App JWT instead of taking it as a known argument.
    """
    async with aiohttp.ClientSession(loop=asyncio.get_running_loop()) as session:
        gh = gh_aiohttp.GitHubAPI(session, __name__)

        jwt = get_jwt(
            app_id=str(SETTINGS.GITHUB_APP_ID),
            private_key=SETTINGS.GITHUB_PRIVATE_KEY,
        )

        await gh.getitem("/app", jwt=jwt)
        record_api_call(endpoint="/app")

        installation_data = await gh.getitem(f"/repos/{repo}/installation", jwt=jwt)
        record_api_call(endpoint=f"/repos/{repo}/installation")
        installation_id = int(installation_data["id"])

        token = await get_access_token(gh, installation_id)

        gh = gh_aiohttp.GitHubAPI(
            session,
            __name__,
            oauth_token=token,
            cache=httpcache,
        )

        yield gh, installation_id


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


@app.command("backfill-pull-requests")
def backfill_pull_requests(
    repo: Annotated[str, typer.Argument(help="Repository in owner/name format")],
    installation: Annotated[int, typer.Argument(help="GitHub App installation ID")],
    path: Annotated[
        Path,
        typer.Option("--path", help="Path to webhook SQLite database file"),
    ] = DEFAULT_WEBHOOK_DB_PATH,
    dry_run: Annotated[
        bool,
        typer.Option("--dry-run", help="List what would be projected, write nothing"),
    ] = False,
):
    """Project all open pull requests of a repository into pr_heads_current.

    `pr_heads_current` is normally only written by `pull_request` webhooks, so an
    open PR that receives no further events stays invisible to evaluation if the
    projection was lost. Run this once after rebuilding the database.
    """
    db_path = path
    if not db_path.exists():
        typer.echo(f"Database does not exist: {db_path}")
        raise typer.Exit(code=1)

    async def handle() -> int:
        async with installation_client(installation) as gh:
            api = API(gh, installation)
            pulls = [pull async for pull in api.get_pulls(f"/repos/{repo}")]

        rows = [
            PullRequestHeadRow.from_github_pull_request(pull)
            for pull in pulls
            if pull.state == "open"
        ]
        typer.echo(f"Found {len(rows)} open pull request(s) for {repo}")
        if dry_run:
            for row in rows:
                typer.echo(
                    f"  would project PR #{row.pr_number} head={row.head_sha} "
                    f"base={row.base_ref} draft={bool(row.pr_draft)}"
                )
            return 0

        store = WebhookStore(settings=_store_settings(db_path))
        return store.upsert_pr_heads_from_api(rows=rows, delivery_id="cli-backfill")

    try:
        written = asyncio.run(handle())
    except Exception as exc:  # noqa: BLE001
        typer.echo(f"Backfill failed for {repo}: {exc}")
        raise typer.Exit(code=1)

    if dry_run:
        typer.echo("Dry run, nothing written")
    else:
        typer.echo(f"Projected {written} pull request head(s) into {db_path}")


@app.command("reconcile")
def reconcile(
    repo: Annotated[str, typer.Argument(help="Repository in owner/name format")],
    path: Annotated[
        Path,
        typer.Option("--path", help="Path to webhook SQLite database file"),
    ] = DEFAULT_WEBHOOK_DB_PATH,
    stale_seconds: Annotated[
        int,
        typer.Option(
            "--stale-seconds",
            help="Treat a PR's sentinel check as stuck if it hasn't been "
            "re-evaluated in this many seconds",
        ),
    ] = SETTINGS.PROJECTION_RECONCILE_STALE_SECONDS,
    dry_run: Annotated[
        bool,
        typer.Option("--dry-run", help="List stuck PRs without refreshing them"),
    ] = False,
):
    """Force-refresh sentinel evaluation for open PRs whose check looks stuck.

    Finds open pull requests whose sentinel check is missing, non-terminal, or
    hasn't been re-evaluated in `--stale-seconds`, and re-evaluates them the same
    way the "Re-evaluate now" check-run action does: a fresh API read followed by
    publish. Meant to run on a schedule to recover PRs stuck behind a webhook
    GitHub never delivered or that got dropped before persistence.
    """
    db_path = path
    if not db_path.exists():
        typer.echo(f"Database does not exist: {db_path}")
        raise typer.Exit(code=1)

    store = WebhookStore(settings=_store_settings(db_path))
    candidates = store.get_stale_open_pr_candidates(
        app_id=SETTINGS.GITHUB_APP_ID,
        check_name=SETTINGS.PROJECTION_CHECK_RUN_NAME,
        stale_seconds=stale_seconds,
        repo_full_name=repo,
    )

    typer.echo(f"Found {len(candidates)} stuck open pull request(s) for {repo}")
    for candidate in candidates:
        typer.echo(
            f"  PR #{candidate.pr_number} head={candidate.head_sha} "
            f"sentinel_status={candidate.sentinel_status or 'unknown'} "
            f"last_eval_at={candidate.last_eval_at or 'never'}"
        )

    if not candidates:
        return
    if dry_run:
        typer.echo("Dry run, nothing refreshed")
        return

    async def handle() -> None:
        async with installation_client_for_repo(repo) as (gh, installation_id):

            async def api_factory(_installation_id: int) -> API:
                return API(gh, _installation_id)

            evaluator = ProjectionEvaluator(
                store=store,
                config=SETTINGS,
                api_factory=api_factory,
            )

            for candidate in candidates:
                if candidate.repo_id is None or candidate.head_sha is None:
                    continue
                trigger = ProjectionTrigger(
                    repo_id=candidate.repo_id,
                    repo_full_name=candidate.repo_full_name or repo,
                    head_sha=candidate.head_sha,
                    installation_id=installation_id,
                    event="reconcile",
                    action="stale_sweep",
                    force_api_refresh=True,
                )
                result = await evaluator.evaluate_and_publish(trigger)
                typer.echo(
                    f"  PR #{candidate.pr_number} head={candidate.head_sha} "
                    f"-> {result.result}"
                )

    try:
        asyncio.run(handle())
    except Exception as exc:  # noqa: BLE001
        typer.echo(f"Reconcile failed for {repo}: {exc}")
        raise typer.Exit(code=1)


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
