from datetime import date, datetime
from functools import cached_property
from typing import List, Literal, Optional, Type, TypeVar
import base64

import pydantic


class Model(pydantic.BaseModel):
    pass


class CommitSha(str):
    @classmethod
    def __get_validators__(cls):
        yield cls.validate_commit_sha

    @classmethod
    def validate_commit_sha(cls, sha: str) -> str:
        if not isinstance(sha, str):
            raise ValueError("Provide a string for CommitSha")
        if len(sha) != 40:
            raise ValueError("Commit hash must have length 40")
        return cls(sha)

    def __repr__(self) -> str:
        return f"CommitSha({super().__repr__()})"

    # def __str__(self) -> str:
    #     return f"CommitSha({super().__str__()})"


class Content(Model):
    type: str
    encoding: Literal["base64"]
    size: int
    name: str
    path: str
    content: str
    sha: str
    url: str
    html_url: str
    download_url: str

    def decoded_content(self) -> str:
        if self.encoding != "base64":
            raise ValueError(f"Unknown encoding {self.encoding}")
        return base64.b64decode(self.content).decode()


class Repository(Model):
    id: int
    name: str
    full_name: Optional[str]
    url: str
    html_url: Optional[str]
    private: Optional[bool]


class PartialPrConnection(Model):
    ref: str
    sha: CommitSha
    repo: Repository


class PrConnection(PartialPrConnection):
    label: str


class PartialPullRequest(Model):
    url: str
    id: int
    number: int
    base: PartialPrConnection
    head: PartialPrConnection


class PullRequest(PartialPullRequest):
    state: Literal["open", "closed"]
    merged_at: Optional[datetime]
    created_at: datetime
    updated_at: datetime
    base: PrConnection
    head: PrConnection
    html_url: Optional[str]

    def __str__(self) -> str:
        name = self.base.repo.name
        if self.base.repo.full_name is not None:
            name = self.base.repo.full_name
        return f"PR({name}#{self.number}, {self.id})"


class App(Model):
    id: int
    slug: str


class PartialCheckSuite(Model):
    id: int

    def __hash__(self):
        return self.id


class CheckSuite(PartialCheckSuite):
    head_branch: Optional[str]
    head_sha: CommitSha
    status: Optional[
        Literal[
            "queued",
            "in_progress",
            "completed",
            "pending",
        ]
    ]
    conclusion: Optional[
        Literal[
            "success",
            "failure",
            "neutral",
            "cancelled",
            "skipped",
            "timed_out",
            "action_required",
        ]
    ]
    url: str
    created_at: datetime
    updated_at: datetime
    check_runs_url: str


class CheckRunOutput(Model):
    title: Optional[str] = None
    summary: Optional[str] = None
    text: Optional[str] = None


class CheckRun(Model):
    id: Optional[int]
    name: str
    head_sha: CommitSha
    status: Literal["completed", "queued", "in_progress"] = "queued"
    conclusion: Optional[
        Literal[
            "action_required",
            "cancelled",
            "failure",
            "neutral",
            "success",
            "skipped",
            "stale",
            "timed_out",
        ]
    ]
    started_at: datetime = pydantic.Field(default_factory=datetime.now)
    completed_at: Optional[datetime]
    pull_requests: List[PartialPullRequest] = pydantic.Field(default_factory=list)
    app: Optional[App] = None
    check_suite: Optional[PartialCheckSuite] = None
    output: Optional[CheckRunOutput] = None
    html_url: Optional[str] = None

    @property
    def is_failure(self) -> bool:
        return self.status == "completed" and self.conclusion in (
            "cancelled",
            "failure",
        )

    @property
    def is_in_progress(self) -> bool:
        return self.status in ("queued", "in_progress")

    @property
    def is_success(self) -> bool:
        return self.status == "completed" and self.conclusion == "success"

    def __hash__(self):
        return self.id

    @classmethod
    def make_app_check_run(cls: Type["CheckRun"], **kwargs) -> "CheckRun":
        return cls(name="merge-sentinel", **kwargs)


class PrFile(Model):
    sha: str
    filename: str
    status: Literal[
        "added", "removed", "modified", "renamed", "copied", "changed", "unchanged"
    ]


class ActionsJob(Model):
    id: int
    url: str
    run_id: int
    run_url: str
    status: Literal["completed", "queued", "in_progress"]
    conclusion: Optional[
        Literal[
            "action_required",
            "cancelled",
            "failure",
            "neutral",
            "success",
            "skipped",
            "stale",
            "timed_out",
        ]
    ]
    name: str
    run_attempt: int


class ActionsRun(Model):
    id: int
    name: str
    head_sha: str
    run_number: int
    event: str
    status: Literal["completed", "queued", "in_progress"] = "queued"
    conclusion: Optional[
        Literal[
            "action_required",
            "cancelled",
            "failure",
            "neutral",
            "success",
            "skipped",
            "stale",
            "timed_out",
        ]
    ]
    workflow_id: int
    check_suite_id: int
    created_at: datetime
    updated_at: datetime


class CommitStatus(Model):
    url: Optional[str]
    id: int
    state: Literal["failure", "pending", "success", "error"]
    created_at: datetime
    updated_at: datetime
    sha: str
    context: str
