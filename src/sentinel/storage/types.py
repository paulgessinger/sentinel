from __future__ import annotations

from datetime import timezone
from typing import Any, Mapping, TYPE_CHECKING, TypeVar

import pydantic

if TYPE_CHECKING:
    from sentinel.github.model import ActionsRun, CheckRun, CommitStatus


ModelT = TypeVar("ModelT", bound="StorageModel")


def _dt_to_iso(value: Any) -> str | None:
    if value is None:
        return None
    if hasattr(value, "astimezone"):
        return value.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%fZ")
    return str(value)


class StorageModel(pydantic.BaseModel):
    model_config = pydantic.ConfigDict(extra="ignore", validate_assignment=True)

    @classmethod
    def from_mapping(cls: type[ModelT], row: Any) -> ModelT:
        if isinstance(row, cls):
            return row
        if isinstance(row, Mapping):
            return cls.model_validate(dict(row))
        return cls.model_validate(row)

    def get(self, key: str, default: Any = None) -> Any:
        return getattr(self, key, default)

    def __getitem__(self, key: str) -> Any:
        return getattr(self, key)

    def __setitem__(self, key: str, value: Any) -> None:
        setattr(self, key, value)

    def to_dict(self) -> dict[str, Any]:
        return self.model_dump(mode="python")


class CheckRunRow(StorageModel):
    check_run_id: int | None
    name: str | None
    status: str | None
    head_sha: str | None = None
    conclusion: str | None = None
    app_id: int | None = None
    app_slug: str | None = None
    check_suite_id: int | None = None
    html_url: str | None = None
    started_at: str | None = None
    completed_at: str | None = None
    repo_id: int | None = None
    repo_full_name: str | None = None
    first_seen_at: str | None = None
    last_seen_at: str | None = None
    last_delivery_id: str | None = None

    @classmethod
    def from_github_check_run(cls, check_run: CheckRun) -> CheckRunRow:
        app = check_run.app
        check_suite = check_run.check_suite
        return cls(
            check_run_id=check_run.id,
            name=check_run.name,
            status=check_run.status,
            conclusion=check_run.conclusion,
            app_id=app.id if app is not None else None,
            app_slug=app.slug if app is not None else None,
            check_suite_id=check_suite.id if check_suite is not None else None,
            html_url=check_run.html_url,
            started_at=_dt_to_iso(check_run.started_at),
            completed_at=_dt_to_iso(check_run.completed_at),
        )


class WorkflowRunRow(StorageModel):
    workflow_run_id: int | None
    name: str | None
    event: str | None = None
    status: str | None = None
    conclusion: str | None = None
    head_sha: str | None = None
    run_number: int | None = None
    workflow_id: int | None = None
    check_suite_id: int | None = None
    app_id: int | None = None
    app_slug: str | None = None
    created_at: str | None = None
    updated_at: str | None = None
    repo_id: int | None = None
    repo_full_name: str | None = None
    first_seen_at: str | None = None
    last_seen_at: str | None = None
    last_delivery_id: str | None = None

    @classmethod
    def from_github_actions_run(cls, workflow_run: ActionsRun) -> WorkflowRunRow:
        return cls(
            workflow_run_id=workflow_run.id,
            name=workflow_run.name,
            event=workflow_run.event,
            status=workflow_run.status,
            conclusion=workflow_run.conclusion,
            run_number=workflow_run.run_number,
            workflow_id=workflow_run.workflow_id,
            check_suite_id=workflow_run.check_suite_id,
            created_at=_dt_to_iso(workflow_run.created_at),
            updated_at=_dt_to_iso(workflow_run.updated_at),
        )


class CommitStatusRow(StorageModel):
    status_id: int | None
    context: str | None
    state: str | None
    sha: str | None = None
    url: str | None = None
    created_at: str | None = None
    updated_at: str | None = None
    repo_id: int | None = None
    repo_full_name: str | None = None
    first_seen_at: str | None = None
    last_seen_at: str | None = None
    last_delivery_id: str | None = None

    @classmethod
    def from_github_commit_status(cls, status: CommitStatus) -> CommitStatusRow:
        return cls(
            status_id=status.id,
            url=status.url,
            sha=status.sha,
            context=status.context,
            state=status.state,
            created_at=_dt_to_iso(status.created_at),
            updated_at=_dt_to_iso(status.updated_at),
        )


class PullRequestHeadRow(StorageModel):
    repo_id: int | None
    repo_full_name: str | None
    pr_id: int | None
    pr_number: int | None
    pr_title: str | None = None
    pr_draft: bool | None = None
    state: str | None = None
    head_sha: str | None = None
    base_ref: str | None = None
    action: str | None = None
    updated_at: str | None = None
    last_delivery_id: str | None = None


class SentinelCheckRunStateRow(StorageModel):
    repo_id: int | None
    repo_full_name: str | None
    head_sha: str | None
    check_name: str | None
    app_id: int | None
    check_run_id: int | None = None
    status: str | None = None
    conclusion: str | None = None
    started_at: str | None = None
    completed_at: str | None = None
    output_title: str | None = None
    output_summary: str | None = None
    output_text: str | None = None
    output_checks_json: str | None = None
    output_summary_hash: str | None = None
    output_text_hash: str | None = None
    last_eval_at: str | None = None
    last_publish_at: str | None = None
    last_publish_result: str | None = None
    last_publish_error: str | None = None
    last_delivery_id: str | None = None


class PRDashboardRow(StorageModel):
    repo_id: int | None
    repo_full_name: str | None
    pr_number: int | None
    pr_id: int | None
    pr_title: str | None
    pr_is_draft: bool
    pr_state: str | None
    pr_merged: bool | None
    head_sha: str | None
    base_ref: str | None
    action: str | None
    pr_updated_at: str | None
    pr_last_delivery_id: str | None
    sentinel_status: str | None
    sentinel_conclusion: str | None
    sentinel_check_run_id: int | None
    output_title: str | None
    output_summary: str | None
    output_text: str | None
    output_checks_json: str | None
    output_summary_hash: str | None
    output_text_hash: str | None
    last_eval_at: str | None
    last_publish_at: str | None
    last_publish_result: str | None
    last_publish_error: str | None
    last_delivery_id: str | None


class RelatedEventRow(StorageModel):
    delivery_id: str
    received_at: str | None
    event: str
    action: str | None
    projection_error: str | None
    detail: str | None
    payload: dict[str, Any] = pydantic.Field(default_factory=dict)
    is_activity: bool = False
    details_url: str | None = None


class WebhookEventRow(StorageModel):
    delivery_id: str
    received_at: str | None
    event: str
    action: str | None
    installation_id: int | None
    repo_id: int | None
    repo_full_name: str | None
    payload: dict[str, Any] = pydantic.Field(default_factory=dict)
    projected_at: str | None
    projection_error: str | None
    detail: str | None
    payload_pretty: str | None = None
    pr_detail_url: str | None = None
