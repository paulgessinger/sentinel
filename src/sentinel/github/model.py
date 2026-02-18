from datetime import datetime
from typing import Any, List, Literal, Type
import base64

import pydantic
from pydantic import GetCoreSchemaHandler
from pydantic_core import core_schema


class Model(pydantic.BaseModel):
    pass


class CommitSha(str):
    @classmethod
    def __get_pydantic_core_schema__(
        cls, source_type: Any, handler: GetCoreSchemaHandler
    ) -> core_schema.CoreSchema:
        return core_schema.no_info_after_validator_function(cls, handler(str))

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
    full_name: str | None
    url: str
    html_url: str | None
    private: bool | None


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
    merged_at: datetime | None
    created_at: datetime
    updated_at: datetime
    base: PrConnection
    head: PrConnection
    html_url: str | None

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
    head_branch: str | None
    head_sha: CommitSha
    status: (
        Literal[
            "queued",
            "in_progress",
            "completed",
            "pending",
        ]
        | None
    )
    conclusion: (
        Literal[
            "success",
            "failure",
            "neutral",
            "cancelled",
            "skipped",
            "timed_out",
            "action_required",
            "startup_failure",
        ]
        | None
    )
    url: str
    created_at: datetime
    updated_at: datetime
    check_runs_url: str


class CheckRunOutput(Model):
    title: str | None = None
    summary: str | None = None
    text: str | None = None


class CheckRunAction(Model):
    label: str
    description: str
    identifier: str


class CheckRun(Model):
    id: int | None
    name: str
    head_sha: CommitSha
    status: Literal["completed", "queued", "in_progress", "pending"] = "queued"
    conclusion: (
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
        | None
    )
    started_at: datetime = pydantic.Field(default_factory=datetime.now)
    completed_at: datetime | None
    pull_requests: List[PartialPullRequest] = pydantic.Field(default_factory=list)
    app: App | None = None
    check_suite: PartialCheckSuite | None = None
    output: CheckRunOutput | None = None
    actions: List[CheckRunAction] = pydantic.Field(default_factory=list)
    html_url: str | None = None

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
    sha: str | None
    filename: str
    status: Literal[
        "added", "removed", "modified", "renamed", "copied", "changed", "unchanged"
    ]


class ActionsJob(Model):
    id: int
    url: str
    run_id: int
    run_url: str
    status: Literal["completed", "queued", "in_progress", "pending"]
    conclusion: (
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
        | None
    )
    name: str
    run_attempt: int


class ActionsRun(Model):
    id: int
    name: str
    head_sha: str
    run_number: int
    event: str
    status: Literal["completed", "queued", "in_progress", "pending"] = "queued"
    conclusion: (
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
        | None
    )
    workflow_id: int
    check_suite_id: int
    created_at: datetime
    updated_at: datetime


class CommitStatus(Model):
    url: str | None
    id: int
    state: Literal["failure", "pending", "success", "error"]
    created_at: datetime
    updated_at: datetime
    sha: str
    context: str
