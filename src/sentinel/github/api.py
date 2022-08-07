from datetime import datetime
from tabnanny import check
from typing import AsyncIterator
from gidgethub.abc import GitHubAPI
import base64

from sentinel.github.model import (
    ActionsJob,
    CheckRun,
    CheckSuite,
    Content,
    PrFile,
    PullRequest,
    Repository,
)

from sanic.log import logger


class API:
    gh: GitHubAPI

    def __init__(self, gh: GitHubAPI):
        self.gh = gh

    async def post_check_run(self, repo_url: str, check_run: CheckRun) -> None:
        fields = {"name", "head_sha", "status", "output", "started_at"}
        if check_run.completed_at is not None:
            fields.add("completed_at")

        if check_run.conclusion is not None:
            fields.add("conclusion")
        payload = check_run.dict(include=fields, exclude_none=False)

        payload["actions"] = []

        for k, v in payload.items():
            if isinstance(v, datetime):
                payload[k] = v.strftime("%Y-%m-%dT%H:%M:%SZ")

        # print(payload)

        if check_run.id is not None:
            url = f"{repo_url}/check-runs/{check_run.id}"
            logger.debug("Updating check run %d, %s", check_run.id, url)
            await self.gh.patch(
                url,
                data=payload,
            )
        else:
            logger.debug("Creating check run")
            await self.gh.post(
                f"{repo_url}/check-runs",
                data=payload,
            )

    async def get_check_runs_for_ref(
        self, repo: Repository, ref: str
    ) -> AsyncIterator[CheckRun]:
        url = f"{repo.url}/commits/{ref}/check-runs"
        logger.debug("Get check runs for ref %s", url)
        async for item in self.gh.getiter(url, iterable_key="check_runs"):
            yield CheckRun.parse_obj(item)

    async def get_check_suites_for_ref(
        self, repo: Repository, ref: str
    ) -> AsyncIterator[CheckSuite]:
        url = f"{repo.url}/commits/{ref}/check-suites"
        logger.debug("Get check runs for ref %s", url)
        async for item in self.gh.getiter(url, iterable_key="check_suites"):
            yield CheckSuite.parse_obj(item)

    async def get_content(self, repo_url: str, path: str) -> Content:
        url = f"{repo_url}/contents/{path}"
        logger.debug("Get file content: %s", url)
        content = Content.parse_obj(await self.gh.getitem(url))

        return content

    async def get_pull_request_files(self, pr: PullRequest) -> AsyncIterator[PrFile]:
        url = f"{pr.base.repo.url}/pulls/{pr.number}/files"
        logger.debug("Getting files for PR #%d %s", pr.number, url)
        async for item in self.gh.getiter(url):
            yield PrFile.parse_obj(item)

    async def get_check_suite(self, repo_url: str, id: int) -> CheckSuite:
        url = f"{repo_url}/check-suites/{id}"
        return CheckSuite.parse_obj(await self.gh.getitem(url))

    async def get_actions_job(self, repo_url: str, id: int) -> ActionsJob:
        url = f"{repo_url}/actions/jobs/{id}"
        return ActionsJob.parse_obj(await self.gh.getitem(url))

    async def get_repository(self, repo_url: str) -> Repository:
        return Repository.parse_obj(await self.gh.getitem(repo_url))

    # async def get_pull_request(self)
