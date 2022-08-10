from datetime import datetime
from tabnanny import check
from typing import AsyncIterator, List
from gidgethub.abc import GitHubAPI
import base64

from sentinel.github.model import (
    ActionsJob,
    CheckRun,
    CheckSuite,
    CommitStatus,
    Content,
    PrFile,
    PullRequest,
    Repository,
)

from sanic.log import logger


class API:
    gh: GitHubAPI
    installation: int

    def __init__(self, gh: GitHubAPI, installation: int):
        self.gh = gh
        self.installation = installation

    async def post_check_run(self, repo_url: str, check_run: CheckRun) -> None:
        fields = {"name", "head_sha", "status", "started_at"}
        if check_run.completed_at is not None:
            fields.add("completed_at")

        if check_run.conclusion is not None:
            fields.add("conclusion")
        payload = check_run.dict(include=fields, exclude_none=False)

        if check_run.output is not None:
            payload["output"] = check_run.output.dict(exclude_none=True)

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
            url = f"{repo_url}/check-runs"
            logger.debug("Creating check run %s on sha %s", url, check_run.head_sha)
            await self.gh.post(
                url,
                data=payload,
            )

    async def get_check_runs_for_ref(
        self, repo: Repository, ref: str
    ) -> AsyncIterator[CheckRun]:
        url = f"{repo.url}/commits/{ref}/check-runs"
        logger.debug("Get check runs for ref %s", url)
        async for item in self.gh.getiter(url, iterable_key="check_runs"):
            yield CheckRun.parse_obj(item)

    async def get_status_for_ref(
        self, repo: Repository, ref: str
    ) -> List[CommitStatus]:
        url = f"{repo.url}/commits/{ref}/status"
        logger.debug("Get commit status for ref %s", url)
        data = await self.gh.getitem(url)
        for item in data["statuses"]:
            yield CommitStatus(sha=data["sha"], **item)

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

    async def get_pulls(self, repo_url: str) -> AsyncIterator[PullRequest]:
        async for item in self.gh.getiter(f"{repo_url}/pulls"):
            yield PullRequest.parse_obj(item)

    async def get_pull(self, repo_url: str, number: int) -> PullRequest:
        url = f"{repo_url}/pulls/{number}"
        logger.debug("Get pull %s", url)
        item = await self.gh.getitem(url)
        return PullRequest.parse_obj(item)

    # async def get_pull_request(self)
