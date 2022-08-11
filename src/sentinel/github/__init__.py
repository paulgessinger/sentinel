import asyncio
from dataclasses import dataclass
from datetime import datetime
from enum import Enum
from fnmatch import fnmatch
import hmac
from importlib.resources import is_resource, path
import io
import logging
from multiprocessing.sharedctypes import Value
from tabnanny import check
from tracemalloc import start
from typing import Any, AsyncIterable, AsyncIterator, Dict, List, Mapping, Optional, Set
import json
from typing_extensions import Required
import gidgethub
import dateutil.parser
from pprint import pprint
import textwrap
import io
from aiolimiter import AsyncLimiter
import aiocache
import re

from gidgethub.routing import Router
from sanic.log import logger
from sanic import Sanic
from gidgethub.abc import GitHubAPI
from gidgethub.sansio import Event
from gidgethub import aiohttp as gh_aiohttp
from gidgethub import BadRequest
import aiohttp
import yaml
from gidgethub.apps import get_installation_access_token, get_jwt
import pydantic
from tabulate import tabulate
import humanize
import pytz
import diskcache

from sentinel import config as app_config
from sentinel.cache import Cache, QueueItem, get_cache
from sentinel.github.api import API
from sentinel.github.model import (
    ActionsJob,
    ActionsRun,
    CheckRun,
    CheckRunOutput,
    CheckSuite,
    CommitStatus,
    PullRequest,
    Repository,
)
from sentinel.model import Config, Rule
from sentinel.metric import webhook_skipped_counter


class ResultStatus(Enum):
    success = 1
    pending = 2
    failure = 3
    missing = 4
    neutral = 5


@dataclass
class ResultItem:
    name: str
    status: ResultStatus

    url: Optional[str] = None
    started_at: Optional[datetime] = None
    completed_at: Optional[datetime] = None
    required: bool = False

    def __hash__(self):
        return hash(self.name)

    def __str__(self):
        if self.url is None:
            return self.name
        else:
            return f"[{self.name}]({self.url})"

    @classmethod
    def from_check_run(cls, cr: CheckRun, **kwargs) -> "ResultItem":
        if cr.is_failure:
            status = ResultStatus.failure
        elif cr.is_in_progress:
            status = ResultStatus.pending
        elif cr.is_success:
            status = ResultStatus.success
        elif cr.status == "completed" and cr.conclusion in ("neutral", "skipped"):
            status = ResultStatus.neutral
        else:
            raise ValueError("Unknown status %s %s", cr.status, cr.conclusion)
        return ResultItem(
            name=cr.name,
            url=cr.html_url,
            status=status,
            started_at=cr.started_at,
            completed_at=cr.completed_at,
            **kwargs,
        )

    @classmethod
    def from_status(cls, cs: CommitStatus, **kwargs) -> "ResultItem":
        started_at = None
        completed_at = None
        if cs.state == "success":
            status = ResultStatus.success
            started_at = cs.created_at
            completed_at = cs.updated_at
        elif cs.state == "pending":
            started_at = cs.created_at
            status = ResultStatus.pending
        elif cs.state == "failure":
            status = ResultStatus.failure
            started_at = cs.created_at
            completed_at = cs.updated_at
        else:
            raise ValueError(f"Invalid commit status state {cs.state}")
        return ResultItem(
            name=cs.context,
            url=cs.url,
            status=status,
            started_at=started_at,
            completed_at=completed_at,
            **kwargs,
        )


class InvalidConfig(Exception):
    raw_config: str
    source_url: str

    def __init__(self, *args, **kwargs):
        self.raw_config = kwargs.pop("raw_config")
        self.source_url = kwargs.pop("source_url")
        super().__init__(*args, **kwargs)


@aiocache.cached(ttl=app_config.ACCESS_TOKEN_TTL, key_builder=lambda fn, gh, id: id)
async def get_access_token(gh: gh_aiohttp.GitHubAPI, installation_id: int) -> str:
    logger.debug("Getting NEW installation access token for %d", installation_id)
    access_token_response = await get_installation_access_token(
        gh,
        installation_id=installation_id,
        app_id=app_config.GITHUB_APP_ID,
        private_key=app_config.GITHUB_PRIVATE_KEY,
    )

    token = access_token_response["token"]
    return token


async def get_config_from_repo(api: API, repo: Repository) -> Optional[Config]:
    try:
        content = await api.get_content(repo.url, ".merge-sentinel.yml")

        if content.type != "file":
            raise ValueError("Config file is not a file")

        decoded_content = content.decoded_content()

        if app_config.OVERRIDE_CONFIG is not None:
            with open(app_config.OVERRIDE_CONFIG) as fh:
                decoded_content = fh.read()

        buf = io.StringIO(decoded_content)
        data = yaml.safe_load(buf)

        try:
            return Config() if data is None else Config.parse_obj(data)
        except pydantic.error_wrappers.ValidationError as e:
            raise InvalidConfig(
                str(e), raw_config=decoded_content, source_url=content.html_url
            )
    except BadRequest as e:
        if e.status_code == 404:
            return None
        raise e


async def populate_check_run(
    api: API,
    pr: PullRequest,
    config: Config,
    check_runs: Set[CheckRun],
    check_run: CheckRun,
) -> CheckRun:

    logger.debug("Have %d checks runs total", len(check_runs))

    changed_files = None

    is_gha = lambda cr: cr.app.id == 15368 and cr.app.slug == "github-actions"

    gha_runs = [cr for cr in check_runs if is_gha(cr)]

    actions_jobs: Dict[int, ActionsJob] = {
        j.id: j
        for j in await asyncio.gather(
            *(api.get_actions_job(pr.base.repo.url, r.id) for r in gha_runs)
        )
    }

    # print("GHA Jobs")
    # for aj in actions_jobs.values():
    #     print("-", aj.id, aj.run_id, aj.status, aj.conclusion, aj.name)

    actions_runs = {
        r["id"]: ActionsRun.parse_obj(r)
        for r in await asyncio.gather(
            *(api.gh.getitem(url) for url in {j.run_url for j in actions_jobs.values()})
        )
    }
    api.call_count += len(actions_jobs.values())

    active_actions_runs: Dict[int, ActionsRun] = {}

    for ar in actions_runs.values():
        if ex_ar := active_actions_runs.get(ar.id):
            if ar.created_at > ex_ar.created_at:
                active_actions_runs[ar.id] = ar
        else:
            active_actions_runs[ar.id] = ar

    # print("All")
    # for ar in actions_runs.values():
    #     print("-", ar.id, ar.created_at, ar.name, ar.status, ar.conclusion)

    # print("Active")
    # for ar in active_actions_runs.values():
    #     print("-", ar.id, ar.created_at, ar.name, ar.status, ar.conclusion)

    for cr in check_runs:
        if cr.id in actions_jobs:
            job = actions_jobs[cr.id]
            run = actions_runs[job.run_id]
            cr.name = f"{run.name} / {cr.name}"

    for cr in list(check_runs):
        if job := actions_jobs.get(cr.id):
            run = actions_runs[job.run_id]
            if run.event not in ("pull_request", "pull_request_target"):
                logger.debug(
                    "Removing check run %d %s due to trigger event %s",
                    cr.id,
                    cr.name,
                    run.event,
                )
                check_runs.remove(cr)

    check_runs_filtered: Dict[str, CheckRun] = {}
    for cr in check_runs:
        if ex_cr := check_runs_filtered.get(cr.name):
            if cr.completed_at is None or ex_cr.completed_at is None:
                check_runs_filtered[cr.name] = cr
            elif (
                ex_cr.completed_at is not None and cr.completed_at > ex_cr.completed_at
            ):
                check_runs_filtered[cr.name] = cr

        else:
            check_runs_filtered[cr.name] = cr

    check_runs = set(check_runs_filtered.values())

    logger.debug("Have %d checks runs to consider", len(check_runs))

    successful_check_run_names = {
        cr.name
        for cr in check_runs
        if cr.completed_at is not None and cr.conclusion == "success"
    }

    statuses = [s async for s in api.get_status_for_ref(pr.base.repo, ref=pr.head.sha)]

    logger.debug("Considered statuses:")
    if logger.getEffectiveLevel() == logging.DEBUG:
        for s in statuses:
            logger.debug("- %d : [%s] %s", s.id, s.updated_at, s.context)

    check_runs_by_name = {cr.name: cr for cr in check_runs}

    logger.debug("Considered check runs:")
    if logger.getEffectiveLevel() == logging.DEBUG:
        for cr in check_runs:
            logger.debug("- %d : [%s] %s", cr.id, cr.completed_at, cr.name)
    logger.debug("Successful check runs are: %s", successful_check_run_names)

    changed_files = [f.filename async for f in api.get_pull_request_files(pr)]

    rules = determine_rules(changed_files, pr, config.rules)

    logger.debug("Have %d rules to apply", len(rules))

    all_started_at = [cr.started_at for cr in check_runs if cr.started_at is not None]
    if any(all_started_at):
        check_run.started_at = min(all_started_at)

    result_items: Set[ResultItem] = {
        ResultItem.from_check_run(cr, required=False) for cr in check_runs
    }

    for status in statuses:
        result_items.add(ResultItem.from_status(status))

    for rule in rules:
        if len(rule.required_checks) > 0:
            logger.debug("- required checks: %s", rule.required_checks)

            seen_results = set()

            # for required_check in rule.required_checks:
            for ri in result_items:
                if ri.name in rule.required_checks:
                    ri.required = True
                    logger.debug("Item %s is required", ri.name)
                    seen_results.add(ri.name)

            for missing in rule.required_checks - seen_results:
                result_items.add(
                    ResultItem(name=missing, status=ResultStatus.missing, required=True)
                )

        if len(rule.required_pattern) > 0:
            logger.debug("- required patterns: %s", rule.required_pattern)
            for pattern in rule.required_pattern:
                matched = False
                for ri in result_items:
                    if fnmatch(ri.name, pattern):
                        logger.debug("Item %s is required", ri.name)
                        ri.required = True
                        matched = True
                if not matched:
                    result_items.add(
                        ResultItem(
                            name=pattern, required=True, status=ResultStatus.failure
                        )
                    )
    successful = {
        ri for ri in result_items if ri.status == ResultStatus.success and ri.required
    }
    logger.debug(
        "Have %d required successes and %d extra successes",
        len(successful),
        len(
            [
                ri
                for ri in result_items
                if ri.status == ResultStatus.success and not ri.required
            ]
        ),
    )

    failures = {ri for ri in result_items if ri.status == ResultStatus.failure}
    logger.debug(
        "Have %d failures",
        len(failures),
    )

    in_progress = {
        ri for ri in result_items if ri.status == ResultStatus.pending and ri.required
    }
    logger.debug(
        "Have %d required pending checks and %d not required ones",
        len(in_progress),
        len(
            [
                ri
                for ri in result_items
                if ri.status == ResultStatus.pending and not ri.required
            ]
        ),
    )

    missing = {ri for ri in result_items if ri.status == ResultStatus.missing}
    in_progress |= missing
    logger.debug(
        "Have %d checks that are unaccounted for",
        len(missing),
    )

    text = "# Checks\n"

    rows = []
    for ri in sorted(result_items, key=lambda ri: ri.name):
        status = ri.status.name
        duration = ""
        if ri.status == ResultStatus.pending:
            icon = ":yellow_circle:"
        elif ri.status == ResultStatus.success:
            icon = ":white_check_mark:"
        elif ri.status == ResultStatus.failure:
            icon = ":x:"
        elif ri.status == ResultStatus.neutral:
            icon = ":white_circle:"
        elif ri.status == ResultStatus.missing:
            icon = ":question:"

        if ri.started_at is not None:
            if ri.completed_at is not None:
                duration = (
                    "completed **"
                    + humanize.naturaltime(
                        ri.completed_at.replace(tzinfo=None), when=datetime.utcnow()
                    )
                    + "** in **"
                    + humanize.naturaldelta(ri.completed_at - ri.started_at)
                    + "**"
                )
            else:
                duration = datetime.utcnow() - ri.started_at.replace(tzinfo=None)
                duration = "running for **" + humanize.naturaldelta(duration) + "**"

        if ri.status == ResultStatus.neutral:
            duration = ""
        is_required = "yes" if ri.required else "no"
        rows.append((icon, f"{ri}", status, is_required))

    text += tabulate(
        rows,
        headers=("", "Check", "Status", "Required?"),
        tablefmt="github",
    )

    summary = []

    if len(failures) > 0:
        summary += [f":x: failed: {', '.join(str(ri) for ri in failures)}"]

    if len(in_progress) > 0:
        s = f":yellow_circle: waiting for: "
        names = [f"{ri}" for ri in in_progress]
        summary += [s + ", ".join(sorted(names))]
    if len(successful) > 0:
        summary += [
            ":white_check_mark: successful required checks: "
            f"{', '.join(f'{ri}' for ri in successful)}"
        ]

    if len(failures) > 0:
        logger.debug("Processing as failure")
        check_run.status = "completed"
        check_run.conclusion = "failure"
        all_completed_at = [
            cr.completed_at for cr in check_runs if cr.completed_at is not None
        ]
        if any(all_completed_at):
            check_run.completed_at = max(all_completed_at)
        if len(failures) == 1:
            title = "1 job has failed"
        else:
            title = f"{len(failures)} jobs have failed"
        check_run.output = CheckRunOutput(title=title)
    elif len(in_progress):
        logger.debug("Processing as in progress")
        if check_run.status != "in_progress":
            check_run.id = None  # unset id to make new check run
        check_run.status = "in_progress"
        check_run.conclusion = None
        check_run.completed_at = None
        if len(in_progress) == 1:
            title = "Waiting for 1 job"
        else:
            title = f"Waiting for {len(in_progress)} jobs"
        if len(in_progress) <= 3:
            title += ": " + ", ".join(sorted([ri.name for ri in in_progress]))
        check_run.output = CheckRunOutput(title=title)
    else:
        logger.debug("Processing as success")
        check_run.status = "completed"
        check_run.conclusion = "success"
        check_run.output = CheckRunOutput(
            title=f"All {len([ri for ri in result_items if ri.required])} required jobs successful"
        )
        check_run.completed_at = max(
            cr.completed_at for cr in check_runs if cr.completed_at is not None
        )

    check_run.output.summary = "\n".join(summary)
    check_run.output.text = text

    return check_run


def determine_rules(
    changed_files: List[str], pr: PullRequest, rules: List[Rule]
) -> List[Rule]:
    selected_rules: List[Rule] = []
    for idx, rule in enumerate(rules, start=1):
        logger.debug("Evaluate rule #%d", idx)
        logger.debug("- have %d branch filters", len(rule.branch_filter))
        if len(rule.branch_filter) > 0:
            matching_filters = [
                f for f in rule.branch_filter if fnmatch(pr.base.ref, f)
            ]
            if len(matching_filters) > 0:
                for bfilter in matching_filters:
                    logger.debug(
                        "-- branch filter '%s' matches base branch '%s'",
                        bfilter,
                        pr.base.ref,
                    )
            else:
                logger.debug(
                    "-- no branch filter matches base branch '%s'", pr.base.ref
                )
                continue

        if rule.paths is not None or rule.paths_ignore is not None:

            paths = rule.paths or []
            paths_ignore = rule.paths_ignore or []

            logger.debug("- have %d path filters", len(paths))
            logger.debug("- have %d path ignore filters", len(paths_ignore))

            accepted = rule_apply_changed_files(
                changed_files, paths=rule.paths, paths_ignore=rule.paths_ignore
            )

            if not accepted:
                logger.debug("-- path filters reject rule")
                continue
            else:
                logger.debug("-- path filters accept rule")

        logger.debug("Applying rule #%d", idx)
        selected_rules.append(rule)
    return selected_rules


def rule_apply_changed_files(
    changed_files: List[str],
    paths: Optional[List[str]] = None,
    paths_ignore: Optional[List[str]] = None,
) -> bool:
    if paths is None and paths_ignore is None:
        raise ValueError("Provide at least one filter argument")

    accepted = True

    if paths is not None:
        accepted = (
            accepted
            and len(paths) > 0
            and any(fnmatch(f, p) for f in changed_files for p in paths)
        )

    if paths_ignore is not None:
        matches = [any(fnmatch(f, p) for p in paths_ignore) for f in changed_files]
        # print(matches)
        accepted = accepted and (len(paths_ignore) == 0 or not all(matches))

    return accepted


async def process_pull_request(pr: PullRequest, api: API):
    logger.info("Begin handling %s", pr)
    # get check runs for PR head_sha on base repo
    check_suites = [
        cs async for cs in api.get_check_suites_for_ref(pr.base.repo, pr.head.sha)
    ]

    logger.debug("Check suites:")
    if logger.getEffectiveLevel() == logging.DEBUG:
        for cs in check_suites:
            logger.debug("- %d", cs.id)

    async def load_check_runs(cs: CheckSuite) -> AsyncIterator[CheckRun]:
        api.call_count += 1
        check_runs = [
            CheckRun.parse_obj(raw)
            async for raw in api.gh.getiter(
                cs.check_runs_url, iterable_key="check_runs"
            )
        ]
        logger.debug("CheckSuite %d -> %d check runs", cs.id, len(check_runs))
        return check_runs

    all_check_runs = set(
        sum(await asyncio.gather(*(load_check_runs(cs) for cs in check_suites)), [])
    )

    logger.debug("All check runs (%d):", len(all_check_runs))
    if logger.getEffectiveLevel() == logging.DEBUG:
        for cr in all_check_runs:
            logger.debug(
                "- %d : [%s] %s (PRs: %s)",
                cr.id,
                cr.completed_at,
                cr.name,
                [i.id for i in cr.pull_requests],
            )

    # all_check_runs = {
    #     cr async for cr in api.get_check_runs_for_ref(pr.base.repo, pr.head.sha)
    # }

    check_runs = set(all_check_runs)

    # check_runs = {cr for cr in check_runs if len(cr.pull_requests) > 0}
    # logger.debug("Have %d check runs after PR filter", len(check_runs))

    check_runs = {cr for cr in check_runs if cr.app.id != app_config.GITHUB_APP_ID}
    logger.debug("Have %d check runs after app id filter", len(check_runs))

    # check_runs = list(
    #     more_itertools.unique_justseen(
    #         sorted(check_runs, key=lambda cr: (cr.name, cr.completed_at)),
    #         key=lambda cr: cr.name,
    #     )
    # )

    check_run = None

    for cr in all_check_runs:
        if cr.app.id == app_config.GITHUB_APP_ID:
            check_run = cr
            break

    check_run = check_run or CheckRun.make_app_check_run(head_sha=pr.head.sha)

    try:
        config = await get_config_from_repo(api, pr.base.repo)
    except InvalidConfig as e:
        # print(e)
        logger.debug("Invalid config file: \n%s", e)
        check_run = check_run or CheckRun.make_app_check_run()
        check_run.head_sha = pr.head.sha
        check_run.status = "completed"
        check_run.conclusion = "failure"
        check_run.started_at = datetime.now()
        check_run.completed_at = datetime.now()
        check_run.output = CheckRunOutput(
            title="Invalid config file",
            summary=f"### :x: Config parsing failed with the following error:\n\n```\n{str(e)}\n```"
            f"\n\nConfig file loaded from {e.source_url}.\nRaw config file below:",
            text=f"```yml\n{e.raw_config}\n```",
        )
        await api.post_check_run(pr.base.repo.url, check_run)
        return

    if config is None:
        logger.debug("No config file found on base repository, not reacting to this PR")
        return

    check_run = await populate_check_run(
        api, pr, config, check_runs=check_runs, check_run=check_run
    )

    # print(check_run.output.title)
    # print(check_run.output.summary)
    # print(check_run.output.text)

    logger.debug("Posting check run for PR %d (#%d)", pr.id, pr.number)
    await api.post_check_run(pr.base.repo.url, check_run)

    logger.info("Finished handling %s, API calls: %d", pr, api.call_count)


async def validate_source_repo(api: API, repo: Repository, pr: PullRequest) -> bool:
    if repo.private:
        if app_config.REPO_ALLOWLIST is not None:
            if repo.full_name in app_config.REPO_ALLOWLIST:
                return True
        logger.warning("Webhook triggered on private repository: %s", repo.html_url)
        await api.post_check_run(
            repo.url,
            CheckRun.make_app_check_run(
                head_sha=pr.head.sha,
                status="completed",
                conclusion="neutral",
                output=CheckRunOutput(
                    title="Not available for private repositories",
                    summary="Not available for private repositories",
                ),
            ),
        )
        return False

    return True


def create_router():
    router = Router()

    @router.register("pull_request")
    async def on_pr(event: Event, api: API, app: Sanic):

        pr = PullRequest.parse_obj(event.data["pull_request"])
        # print(pr)
        logger.debug("Received pull_request event on PR #%d", pr.number)

        action = event.data["action"]
        logger.debug("Action: %s", action)

        repo = Repository.parse_obj(event.data["repository"])

        if not await validate_source_repo(api, repo, pr):
            return

        if action not in ("synchronize", "opened", "reopened"):
            return

        with get_cache() as dcache:
            await dcache.push_pr(QueueItem(pr, api.installation))

    @router.register("check_run")
    async def on_check_run(event: Event, api: API, app: Sanic):
        check_run = CheckRun.parse_obj(event.data["check_run"])

        if check_run.app.id == app_config.GITHUB_APP_ID:
            logger.debug("Check run from us, skip handling")
            webhook_skipped_counter.labels(name=check_run.name, event="check_run").inc()
            return

        if app_config.CHECK_RUN_NAME_FILTER is not None:
            if re.match(app_config.CHECK_RUN_NAME_FILTER, check_run.name):
                logger.debug(
                    "Skipping check run '%s' due to filter '%s'",
                    check_run.name,
                    app_config.CHECK_RUN_NAME_FILTER,
                )
                webhook_skipped_counter.labels(
                    name=check_run.name, event="check_run"
                ).inc()
                return

        repo = Repository.parse_obj(event.data["repository"])

        with get_cache() as dcache:
            async with dcache.lock:
                if hit := dcache.get(f"cached_prs_repo_{repo.id}"):
                    prs = hit
                else:
                    logger.info("Getting all PRs for repo %s", repo.url)
                    prs = [pr async for pr in api.get_pulls(repo.url)]
                    dcache.set(
                        f"cached_prs_repo_{repo.id}", prs, expire=app_config.PRS_TTL
                    )

            for pr in prs:
                if check_run.head_sha == pr.head.sha:
                    logger.info(
                        "- Check run %s triggers pushing %s", check_run.name, pr
                    )
                    await dcache.push_pr(QueueItem(pr, api.installation))

    @router.register("status")
    async def on_status(event: Event, api: API, app: Sanic):
        status = CommitStatus.parse_obj(event.data)
        if app_config.CHECK_RUN_NAME_FILTER is not None:
            if re.match(app_config.CHECK_RUN_NAME_FILTER, status.context):
                logger.debug(
                    "Skipping status '%s' due to filter '%s'",
                    status.context,
                    app_config.CHECK_RUN_NAME_FILTER,
                )
                webhook_skipped_counter.labels(
                    name=status.context, event="status"
                ).inc()
                return

        repo = Repository.parse_obj(event.data["repository"])

        with get_cache() as dcache:
            async with dcache.lock:
                if hit := dcache.get(f"cached_prs_repo_{repo.id}"):
                    prs = hit
                else:
                    logger.info("Getting all PRs for repo %s", repo.url)
                    prs = [pr async for pr in api.get_pulls(repo.url)]
                    dcache.set(
                        f"cached_prs_repo_{repo.id}", prs, expire=app_config.PRS_TTL
                    )

            for pr in prs:
                if status.sha == pr.head.sha:
                    logger.info("- Status %s triggers pushing %s", status.context, pr)
                    await dcache.push_pr(QueueItem(pr, api.installation))

    return router
