from unittest.mock import Mock

import pytest
from gidgethub.abc import GitHubAPI
from pydantic import ValidationError

from sentinel.github.api import API
from sentinel.github.model import PrConnection, Repository


@pytest.fixture
def repository_reference():
    return {
        "id": 175391102,
        "name": "acts",
        "url": "https://api.github.com/repos/acts-project/acts",
    }


@pytest.mark.asyncio
async def test_check_runs_accept_abbreviated_pr_repositories(repository_reference):
    sha = "dec97f3a62d50b5617d0b41ad6010a93c5f058d4"
    repo = Repository(
        **repository_reference,
        full_name="acts-project/acts",
        html_url="https://github.com/acts-project/acts",
        private=False,
    )
    payload = {
        "id": 123,
        "name": "build",
        "head_sha": sha,
        "status": "completed",
        "conclusion": "success",
        "completed_at": "2026-11-03T14:35:00Z",
        "pull_requests": [
            {
                "id": 456,
                "number": 6405,
                "url": f"{repo.url}/pulls/6405",
                "base": {"ref": "main", "sha": "a" * 40, "repo": repository_reference},
                "head": {"ref": "feature", "sha": sha, "repo": repository_reference},
            }
        ],
    }

    async def getiter(url, *, iterable_key):
        assert url == f"{repo.url}/commits/{sha}/check-runs"
        assert iterable_key == "check_runs"
        yield payload

    api = API(Mock(spec=GitHubAPI, getiter=getiter), installation=1)
    runs = [run async for run in api.get_check_runs_for_ref(repo, sha)]
    assert len(runs) == 1
    assert runs[0].conclusion == "success"
    pr = runs[0].pull_requests[0]
    assert pr.number == 6405
    assert pr.base.repo.id == repo.id
    assert pr.head.repo.url == repo.url


def test_full_pr_connection_still_requires_full_repository(repository_reference):
    with pytest.raises(ValidationError) as exc:
        PrConnection.model_validate(
            {
                "ref": "main",
                "sha": "a" * 40,
                "label": "acts-project:main",
                "repo": repository_reference,
            }
        )

    assert {error["loc"] for error in exc.value.errors()} == {
        ("repo", "full_name"),
        ("repo", "html_url"),
        ("repo", "private"),
    }
