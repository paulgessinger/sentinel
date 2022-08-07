import asyncio
from datetime import datetime
from fnmatch import fnmatch
import hmac
from importlib.resources import path
import io
import logging
from tabnanny import check
from tracemalloc import start
from typing import Any, AsyncIterable, AsyncIterator, Dict, List, Mapping, Optional, Set
import json
import gidgethub
import dateutil.parser
from pprint import pprint
import textwrap
import io

from gidgethub.routing import Router
from sanic.log import logger
from sanic import Sanic
from gidgethub.abc import GitHubAPI
from gidgethub.sansio import Event
from gidgethub import BadRequest
import aiohttp
import yaml
import pydantic
from tabulate import tabulate
import humanize
import pytz

from sentinel import config as app_config
from sentinel.github.api import API
from sentinel.github.model import (
    ActionsJob,
    ActionsRun,
    CheckRun,
    CheckRunOutput,
    CheckSuite,
    PullRequest,
    Repository,
)
from sentinel.model import Config, Rule


class InvalidConfig(Exception):
    raw_config: str
    source_url: str

    def __init__(self, *args, **kwargs):
        self.raw_config = kwargs.pop("raw_config")
        self.source_url = kwargs.pop("source_url")
        super().__init__(*args, **kwargs)


async def get_config_from_repo(api: API, repo: Repository) -> Optional[Config]:
    try:
        content = await api.get_content(repo.url, ".merge-sentinel.yml")

        if content.type != "file":
            raise ValueError("Config file is not a file")

        decoded_content = content.decoded_content()

        with open("test_config.yml") as fh:
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

    print("GHA Jobs")
    for aj in actions_jobs.values():
        print("-", aj.id, aj.run_id, aj.status, aj.conclusion, aj.name)

    actions_runs = {
        r["id"]: ActionsRun.parse_obj(r)
        for r in await asyncio.gather(
            *(api.gh.getitem(url) for url in {j.run_url for j in actions_jobs.values()})
        )
    }

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
            if cr.completed_at is None:
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

    check_runs_by_name = {cr.name: cr for cr in check_runs}

    logger.debug("Considered check runs:")
    if logger.getEffectiveLevel() == logging.DEBUG:
        for cr in check_runs:
            logger.debug("- %d : [%s] %s", cr.id, cr.completed_at, cr.name)
    logger.debug("Successful check runs are: %s", successful_check_run_names)

    changed_files = [f.filename async for f in api.get_pull_request_files(pr)]

    rules = determine_rules(changed_files, pr, config.rules)

    logger.debug("Have %d rules to apply", len(rules))

    check_run.started_at = min(
        cr.started_at for cr in check_runs if cr.started_at is not None
    )

    failures = {cr for cr in check_runs if cr.is_failure}
    in_progress = set()
    missing_checks = set()
    successful = set()

    all_required: Set[str] = set()

    for rule in rules:
        if len(rule.required_checks) > 0:
            logger.debug("- required checks: %s", rule.required_checks)

            all_required |= rule.required_checks

            rule_missing_checks = rule.required_checks - successful_check_run_names
            if len(rule_missing_checks) == 0:
                logger.debug("-- no missing successful checks")
            else:
                logger.debug("-- missing successful checks: %s", rule_missing_checks)

            successful |= {
                check_runs_by_name[n]
                for n in rule.required_checks & successful_check_run_names
            }

            for missing_check in list(rule_missing_checks):
                logger.debug("Find missing check '%s'", missing_check)
                if check := check_runs_by_name.get(missing_check):
                    logger.debug("--- missing check found: '%s'", check.name)
                    if check.is_failure:
                        failures.add(check)
                        rule_missing_checks.remove(missing_check)
                    else:
                        in_progress.add(check)
                        rule_missing_checks.remove(missing_check)
            missing_checks |= rule_missing_checks

        if len(rule.required_pattern) > 0:
            logger.debug("- required patterns: %s", rule.required_pattern)
            for cr in check_runs:
                matching_patterns = list(
                    filter(lambda p: fnmatch(cr.name, p), rule.required_pattern)
                )
                if len(matching_patterns) == 0:
                    continue

                logger.debug(
                    "-- check run %s matches pattern(s): %s", cr.name, matching_patterns
                )

                all_required.add(cr.name)

                if cr.is_failure:
                    failures.add(cr)
                elif cr.is_in_progress:
                    in_progress.add(cr)
                elif cr.is_success:
                    successful.add(cr)
                elif cr.status == "completed" and cr.conclusion == "skipped":
                    pass
                else:
                    logger.error("Not sure what to do with cr %s", cr)

    logger.debug("Have %d required successes", len(successful))
    logger.debug("Have %d failures", len(failures))
    logger.debug("Have %d in progress checks", len(in_progress))
    logger.debug("Have %d checks that are unaccounted for", len(missing_checks))

    text = "# Checks\n"

    rows = []
    for cr in sorted(check_runs, key=lambda cr: cr.name):
        if cr.is_in_progress:
            icon = ":yellow_circle:"
            status = cr.status
            duration = datetime.utcnow() - cr.started_at.replace(tzinfo=None)
            duration = "running for **" + humanize.naturaldelta(duration) + "**"
        elif cr.status == "completed":
            status = cr.conclusion
            duration = (
                "completed **"
                + humanize.naturaltime(
                    cr.completed_at.replace(tzinfo=None), when=datetime.utcnow()
                )
                + "** in **"
                + humanize.naturaldelta(cr.completed_at - cr.started_at)
                + "**"
            )
            if cr.conclusion == "skipped":
                icon = ":fast_forward:"
            elif cr.conclusion == "failure":
                icon = ":x:"
            elif cr.conclusion == "cancelled":
                icon = ":white_circle:"
            elif cr.conclusion == "success":
                icon = ":heavy_check_mark:"
            else:
                icon = ":question:"

        is_required = "yes" if cr.name in all_required else "no"
        rows.append(
            (icon, f"[{cr.name}]({cr.html_url})", status, duration, is_required)
        )

    text += tabulate(
        rows,
        headers=("", "Check", "Status", "Duration", "Required?"),
        tablefmt="github",
    )

    if len(missing_checks) > 0:
        text += "\n".join(
            ["", "# Missing required checks:"]
            + ["- :question: " + c for c in sorted(missing_checks)]
        )

    summary = []

    failures = list(sorted(failures, key=lambda f: f.name))
    in_progress_names = list(sorted(missing_checks) + [c.name for c in in_progress])
    successful = list(sorted(successful, key=lambda cr: cr.name))

    if len(failures) > 0:
        summary += [f":x: failed: {', '.join(cr.name for cr in failures)}"]

    if len(in_progress) > 0 or len(missing_checks) > 0:
        s = f":yellow_circle: waiting for: "
        names = list(missing_checks) + [
            f"[{cr.name}]({cr.html_url})" for cr in in_progress
        ]
        summary += [s + ", ".join(sorted(names))]
    if len(successful) > 0:
        summary += [
            ":heavy_check_mark: successful required checks: "
            f"{', '.join(f'[{cr.name}]({cr.html_url})' for cr in successful)}"
        ]

    if len(failures) > 0:
        logger.debug("Processing as failure")
        check_run.status = "completed"
        check_run.conclusion = "failure"
        check_run.completed_at = max(
            cr.completed_at for cr in check_runs if cr.completed_at is not None
        )
        if len(failures) == 1:
            title = "1 job has failed"
        else:
            title = f"{len(failures)} jobs have failed"
        check_run.output = CheckRunOutput(title=title)
    elif len(in_progress) > 0 or len(missing_checks) > 0:
        logger.debug("Processing as in progress")
        if check_run.status != "in_progress":
            check_run.id = None  # unset id to make new check run
        check_run.status = "in_progress"
        check_run.conclusion = None
        check_run.completed_at = None
        if len(in_progress_names) == 1:
            title = "Waiting for 1 job"
        else:
            title = f"Waiting for {len(in_progress_names)} jobs"
        if len(in_progress_names) <= 3:
            title += ": " + ", ".join(in_progress_names)
        check_run.output = CheckRunOutput(title=title)
    else:
        logger.debug("Processing as success")
        check_run.status = "completed"
        check_run.conclusion = "success"
        check_run.output = CheckRunOutput(
            title=f"All {len(all_required)} required jobs successful"
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


async def process_pull_request(pr: PullRequest, api: API, app: Sanic):
    logger.debug("Begin handling PR %d", pr.id)
    # get check runs for PR head_sha on base repo
    check_suites = [
        cs async for cs in api.get_check_suites_for_ref(pr.base.repo, pr.head.sha)
    ]

    logger.debug("Check suites:")
    if logger.getEffectiveLevel() == logging.DEBUG:
        for cs in check_suites:
            logger.debug("- %d", cs.id)

    async def load_check_runs(cs: CheckSuite) -> AsyncIterator[CheckRun]:
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

    logger.debug("All check runs:")
    if logger.getEffectiveLevel() == logging.DEBUG:
        for cr in all_check_runs:
            logger.debug("- %d : [%s] %s", cr.id, cr.completed_at, cr.name)

    # all_check_runs = {
    #     cr async for cr in api.get_check_runs_for_ref(pr.base.repo, pr.head.sha)
    # }

    check_runs = {
        cr
        for cr in all_check_runs
        if len(cr.pull_requests) > 0 and cr.app.id != app_config.GITHUB_APP_ID
    }

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
    await api.post_check_run(pr.base.repo.url, check_run)

    logger.debug("Finished handling PR %d", pr.id)


async def pull_request_update_task(pr: PullRequest, api: API, app: Sanic):
    async with app.ctx.queue_lock:
        app.ctx.pull_request_queue.add(pr.id)
    try:
        start = datetime.now()
        await process_pull_request(pr, api, app)
        duration = (datetime.now() - start).total_seconds()
        # print(app_config.MAX_PR_FREQUENCY)
        min_duration = 1.0 / app_config.MAX_PR_FREQUENCY
        if duration < min_duration:
            logger.debug(
                "Update duration %.2f shorter than min duration %.2f, sleeping for %.2f",
                duration,
                min_duration,
                min_duration - duration,
            )
            await asyncio.sleep(min_duration - duration)
    except:
        logger.error("Exception raised when processing pull request", exc_info=True)
        raise
    finally:
        async with app.ctx.queue_lock:
            app.ctx.pull_request_queue.remove(pr.id)


async def enqueue_pull_request(pr: PullRequest, api: API, app: Sanic):
    async with app.ctx.queue_lock:
        if pr.id in app.ctx.pull_request_queue:
            logger.debug("Pull request id %s is already updating", pr.id)
            return

    app.add_task(pull_request_update_task(pr, api, app))


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

        if repo.private:
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
            return

        if action not in ("synchronize", "opened", "reopened"):
            return

        await enqueue_pull_request(pr, api, app)

    @router.register("check_run")
    async def on_check_run(event: Event, api: API, app: Sanic):
        check_run = CheckRun.parse_obj(event.data["check_run"])

        if check_run.app.id == app_config.GITHUB_APP_ID:
            logger.debug("Check run from us, skip handling")
            return

        for pr in check_run.pull_requests:
            # print(pr.number)
            await enqueue_pull_request(pr, api, app)

    return router
