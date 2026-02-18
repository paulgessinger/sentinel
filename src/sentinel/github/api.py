from datetime import datetime, timezone
from typing import AsyncIterator, List
from gidgethub.abc import GitHubAPI
import copy

from sentinel.github.model import (
    ActionsJob,
    ActionsRun,
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

    call_count: int

    def __init__(self, gh: GitHubAPI, installation: int):
        self.gh = gh
        self.installation = installation
        self.call_count = 0

    async def post_check_run(self, repo_url: str, check_run: CheckRun) -> int | None:
        self.call_count += 1
        fields = {"name", "head_sha", "status", "started_at"}
        if check_run.completed_at is not None:
            fields.add("completed_at")

        if check_run.conclusion is not None:
            fields.add("conclusion")
        payload = check_run.model_dump(include=fields, exclude_none=False)

        if check_run.output is not None:
            payload["output"] = check_run.output.model_dump(exclude_none=True)

        if check_run.actions:
            payload["actions"] = [action.model_dump(exclude_none=True) for action in check_run.actions]

        payload_pre = copy.deepcopy(payload)

        for k, v in payload.items():
            if isinstance(v, datetime):
                # Have issue with datetime 1-01-01T00:00:00Z
                # Not sure where this came from but let's not push it to the GH API
                if (datetime.now(timezone.utc) - v).days > 100 * 365:
                    payload[k] = datetime.now().strftime("%Y-%m-%dT%H:%M:%SZ")
                    logger.warning(
                        "Skipping datetime: %s (very old, possibly invalid), using %s instead",
                        v,
                        payload[k],
                    )
                else:
                    payload[k] = v.strftime("%Y-%m-%dT%H:%M:%SZ")
                logger.debug("Converting datetime: %s -> %s", v, payload[k])

        try:

            if check_run.id is not None:
                url = f"{repo_url}/check-runs/{check_run.id}"
                logger.debug("Updating check run %d, %s", check_run.id, url)
                response = await self.gh.patch(
                    url,
                    data=payload,
                )
            else:
                url = f"{repo_url}/check-runs"
                logger.debug("Creating check run %s on sha %s", url, check_run.head_sha)
                response = await self.gh.post(
                    url,
                    data=payload,
                )
            if isinstance(response, dict):
                run_id = response.get("id")
                return None if run_id is None else int(run_id)
            return None
        except:
            logger.error("Error patch/post with payload: %s", payload)
            logger.error("           -> pre payload was: %s", payload_pre)
            raise

    async def find_existing_sentinel_check_run(
        self,
        *,
        repo_url: str,
        head_sha: str,
        check_name: str,
        app_id: int | None = None,
    ) -> CheckRun | None:
        self.call_count += 1
        url = f"{repo_url}/commits/{head_sha}/check-runs"
        async for item in self.gh.getiter(url, iterable_key="check_runs"):
            check_run = CheckRun.model_validate(item)
            if check_run.name != check_name:
                continue
            if app_id is not None:
                app = check_run.app
                if app is None or app.id != app_id:
                    continue
            return check_run
        return None

    async def get_check_runs_for_ref(
        self, repo: Repository, ref: str
    ) -> AsyncIterator[CheckRun]:
        self.call_count += 1
        url = f"{repo.url}/commits/{ref}/check-runs"
        logger.debug("Get check runs for ref %s", url)
        async for item in self.gh.getiter(url, iterable_key="check_runs"):
            yield CheckRun.model_validate(item)

    async def get_status_for_ref(
        self, repo: Repository, ref: str
    ) -> List[CommitStatus]:
        self.call_count += 1
        url = f"{repo.url}/commits/{ref}/status"
        logger.debug("Get commit status for ref %s", url)
        data = await self.gh.getitem(url)
        for item in data["statuses"]:
            yield CommitStatus(sha=data["sha"], **item)

    async def get_check_suites_for_ref(
        self, repo: Repository, ref: str
    ) -> AsyncIterator[CheckSuite]:
        self.call_count += 1
        url = f"{repo.url}/commits/{ref}/check-suites"
        logger.debug("Get check runs for ref %s", url)
        async for item in self.gh.getiter(url, iterable_key="check_suites"):
            yield CheckSuite.model_validate(item)

    async def get_workflow_runs_for_ref(
        self, repo: Repository, ref: str
    ) -> AsyncIterator[ActionsRun]:
        self.call_count += 1
        url = f"{repo.url}/actions/runs?head_sha={ref}"
        logger.debug("Get workflow runs for ref %s", url)
        async for item in self.gh.getiter(url, iterable_key="workflow_runs"):
            yield ActionsRun.model_validate(item)

    async def get_content(self, repo_url: str, path: str) -> Content:
        self.call_count += 1
        url = f"{repo_url}/contents/{path}"
        logger.debug("Get file content: %s", url)
        content = Content.model_validate(await self.gh.getitem(url))

        return content

    async def get_pull_request_files(self, pr: PullRequest) -> AsyncIterator[PrFile]:
        self.call_count += 1
        url = f"{pr.base.repo.url}/pulls/{pr.number}/files"
        logger.debug("Getting files for PR #%d %s", pr.number, url)
        async for item in self.gh.getiter(url):
            yield PrFile.model_validate(item)

    async def get_check_suite(self, repo_url: str, id: int) -> CheckSuite:
        self.call_count += 1
        url = f"{repo_url}/check-suites/{id}"
        return CheckSuite.model_validate(await self.gh.getitem(url))

    async def get_actions_job(self, repo_url: str, id: int) -> ActionsJob:
        self.call_count += 1
        url = f"{repo_url}/actions/jobs/{id}"
        return ActionsJob.model_validate(await self.gh.getitem(url))

    async def get_repository(self, repo_url: str) -> Repository:
        self.call_count += 1
        return Repository.model_validate(await self.gh.getitem(repo_url))

    async def get_pulls(self, repo_url: str) -> AsyncIterator[PullRequest]:
        self.call_count += 1
        async for item in self.gh.getiter(f"{repo_url}/pulls"):
            yield PullRequest.model_validate(item)

    async def get_pull(self, repo_url: str, number: int) -> PullRequest:
        self.call_count += 1
        url = f"{repo_url}/pulls/{number}"
        logger.debug("Get pull %s", url)
        item = await self.gh.getitem(url)
        return PullRequest.model_validate(item)

    # async def get_pull_request(self)
