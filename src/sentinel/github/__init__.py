import asyncio
from datetime import datetime
from fnmatch import fnmatch
import hmac
from importlib.resources import path
import io
from tabnanny import check
from tracemalloc import start
from typing import Any, Dict, List, Mapping, Optional, Set
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

from sentinel import config as app_config
from sentinel.github.api import API
from sentinel.github.model import (
    ActionsJob,
    ActionsRun,
    CheckRun,
    CheckRunOutput,
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


async def create_check_run(
    api: API,
    pr: PullRequest,
    config: Config,
    check_runs: Set[CheckRun],
    check_run: CheckRun,
) -> CheckRun:

    logger.debug("Have %d checks runs total", len(check_runs))

    # successful_check_runs = {
    #     cr
    #     for cr in check_runs
    #     if cr.completed_at is not None and cr.conclusion == "success"
    # }

    # partial_check_suites = {cr.check_suite for cr in successful_check_runs}

    # check_suites = {
    #     cs.id: cs
    #     for cs in await asyncio.gather(
    #         *(
    #             api.get_check_suite(pr.base.repo.url, pcs.id)
    #             for pcs in partial_check_suites
    #         )
    #     )
    # }

    changed_files = None

    is_gha = lambda cr: cr.app.id == 15368 and cr.app.slug == "github-actions"

    gha_runs = [cr for cr in check_runs if is_gha(cr)]

    actions_jobs: Dict[int, ActionsJob] = {
        j.id: j
        for j in await asyncio.gather(
            *(api.get_actions_job(pr.base.repo.url, r.id) for r in gha_runs)
        )
    }

    # print({j.run_url for j in actions_jobs})

    actions_runs = {
        r["id"]: ActionsRun.parse_obj(r)
        for r in await asyncio.gather(
            *(api.gh.getitem(url) for url in {j.run_url for j in actions_jobs.values()})
        )
    }

    # print(actions_runs)

    for cr in check_runs:
        if cr.id in actions_jobs:
            job = actions_jobs[cr.id]
            run = actions_runs[job.run_id]
            cr.name = f"{run.name} / {cr.name}"

    for cr in list(check_runs):
        if job := actions_jobs.get(cr.id):
            run = actions_runs[job.run_id]
            if run.event != "pull_request":
                check_runs.remove(cr)

    logger.debug("Have %d checks runs to consider", len(check_runs))

    successful_check_run_names = {
        cr.name
        for cr in check_runs
        if cr.completed_at is not None and cr.conclusion == "success"
    }

    check_runs_by_name = {cr.name: cr for cr in check_runs}

    logger.debug("All check runs: %s", [cr.name for cr in check_runs])
    logger.debug("Successful check runs are: %s", successful_check_run_names)

    rules: List[Rule] = []

    for idx, rule in enumerate(config.rules, start=1):
        logger.debug("Evaluate rule #%d", idx)
        logger.debug("- have %d branch filters", len(rule.branch_filter))
        if len(rule.branch_filter) > 0:
            matching_filters = [
                f for f in rule.branch_filter if fnmatch(pr.base.ref, f)
            ]
            if len(matching_filters) > 0:
                for filter in matching_filters:
                    logger.debug(
                        "-- branch filter '%s' matches base branch '%s'",
                        filter,
                        pr.base.ref,
                    )
            else:
                logger.debug(
                    "-- no branch filter matches base branch '%s'", pr.base.ref
                )
                continue

        logger.debug("- have %d path filters", len(rule.path_filter))
        if len(rule.path_filter) > 0:
            if changed_files is None:
                changed_files = [f async for f in api.get_pull_request_files(pr)]

            accepted = True
            for path_filter in rule.path_filter:
                path_filter = path_filter.strip()
                if path_filter.startswith("!"):
                    path_filter = path_filter[1:]
                    logger.debug(
                        "-- checking negative filter '%s' against %d files",
                        path_filter,
                        len(changed_files),
                    )

                    filter_accepted = not any(
                        fnmatch(file.filename, path_filter) for file in changed_files
                    )
                    logger.debug(
                        "--- negative path filter %s",
                        "accepts" if filter_accepted else "does not accept",
                    )
                    accepted = accepted and filter_accepted
                else:
                    logger.debug(
                        "-- checking positive filter '%s' against %d files",
                        path_filter,
                        len(changed_files),
                    )

                    filter_accepted = all(
                        fnmatch(file.filename, path_filter) for file in changed_files
                    )

                    logger.debug(
                        "--- positive path filter %s",
                        "accepts" if filter_accepted else "does not accept",
                    )
                    accepted = accepted and filter_accepted

            if not accepted:
                logger.debug("-- path filters reject rule")
                continue
            else:
                logger.debug("-- path filters accept rule")

        logger.debug("Applying rule #%d", idx)
        rules.append(rule)

    logger.debug("Have %d rules to apply", len(rules))

    failures = {cr for cr in check_runs if cr.is_failure}
    in_progress = set()
    for rule in rules:
        if len(rule.required_checks) > 0:
            logger.debug("- required checks: %s", rule.required_checks)

            missing_checks = rule.required_checks - successful_check_run_names
            if len(missing_checks) == 0:
                logger.debug("-- no missing successful checks")
            else:
                logger.debug("-- missing successful checks: %s", missing_checks)

            for missing_check in list(missing_checks):
                logger.debug("Find missing check '%s'", missing_check)
                if check := check_runs_by_name.get(missing_check):
                    logger.debug("--- missing check found: '%s'", check)
                    if check.is_failure:
                        failures.add(check)
                        missing_checks.remove(missing_check)
                    else:
                        in_progress.add(check)
                        missing_checks.remove(missing_check)

    logger.debug("Have %d failures", len(failures))
    logger.debug("Have %d in progress checks", len(in_progress))
    logger.debug("Have %d checks that are unaccounted for", len(missing_checks))

    if len(failures) > 0:
        check_run.status = "completed"
        check_run.conclusion = "failure"
        check_run.completed_at = datetime.now()
        failure_names = [c.name for c in sorted(failures, key=lambda f: f.name)]
        check_run.output = CheckRunOutput(
            title=f"{len(failure_names)} jobs have failed",
            summary=f"failed: {', '.join(failure_names)}",
        )
    else:
        if len(in_progress) > 0 or len(missing_checks) > 0:
            check_run.status = "in_progress"
            names = [
                c.name
                for c in sorted(in_progress + missing_checks, key=lambda c: c.name)
            ]
            check_run.output = CheckRunOutput(
                title=f"waiting for {len(names)} jobs",
                summary=f"waiting for: {', '.join(names)}",
            )
    check_run.output.summary = check_run.output.title

    return check_run


def create_router():
    router = Router()

    @router.register("pull_request")
    async def on_pr(event: Event, api: API, app: Sanic):

        pr = PullRequest.parse_obj(event.data["pull_request"])
        # print(pr)
        logger.debug("Received pull_request event on PR #%d", pr.number)

        action = event.data["action"]
        logger.debug("Action: %s", action)

        if action not in ("synchronize", "opened", "reopened"):
            return

        # get check runs for PR head_sha on base repo
        all_check_runs = {
            cr async for cr in api.get_check_runs_for_ref(pr.base.repo, pr.head.sha)
        }

        check_runs = {
            cr
            for cr in all_check_runs
            if len(cr.pull_requests) > 0 and cr.app.id != app_config.GITHUB_APP_ID
        }

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
            logger.debug("Invalid config file")
            check_run = check_run or CheckRun.make_app_check_run(
                head_sha=pr.head.sha, status="completed", conclusion="failure"
            )
            check_run.output = CheckRunOutput(
                title="Invalid config file",
                summary=f"### :x: Config parsing failed with the following error:\n\n```\n{str(e)}\n```"
                f"\n\nConfig file loaded from {e.source_url}.\nRaw config file below:",
                text=f"```yml\n{e.raw_config}\n```",
            )
            await api.post_check_run(pr.base.repo.url, check_run)
            return

        print(config)

        if config is None:
            logger.debug(
                "No config file found on base repository, not reacting to this PR"
            )
            return

        check_run = await create_check_run(
            api, pr, config, check_runs=check_runs, check_run=check_run
        )

        print(check_run.output.title)
        await api.post_check_run(pr.base.repo.url, check_run)

    # @router.register("check_run")
    # async def on_check_run(event: Event, api: API, app: Sanic):
    #     check_run = CheckRun.parse_obj(event.data["check_run"])

    #     if check_run.app.id == config.GITHUB_APP_ID:
    #         logger.debug("Check run from us, skip handling")
    #         return

    #     print(check_run)
    # await handle_rerequest(gh, app.ctx.aiohttp_session, event.data)

    return router
