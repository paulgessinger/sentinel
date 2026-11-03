from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class ProjectionTrigger:
    repo_id: int
    repo_full_name: str
    head_sha: str
    installation_id: int
    delivery_id: str | None = None
    event: str | None = None
    action: str | None = None
    force_api_refresh: bool = False
    pre_delay_pending: bool = False
    # Set when the merge queue entry this SHA belongs to was destroyed: the
    # target no longer resolves, so evaluation cannot run, but a non-terminal
    # sentinel check run must still be closed out instead of hanging forever.
    finalize_abandoned: bool = False
    # A manual refresh should produce a visibly fresh check run, but an automated
    # sweep must keep updating the existing one: replacing a merge queue entry's
    # required check every sweep resets it to in_progress and churns the queue.
    allow_new_check_run: bool = True

    @property
    def key(self) -> str:
        return f"{self.repo_id}:{self.head_sha}"


@dataclass(frozen=True)
class EvaluationResult:
    result: str
    check_run_id: int | None = None
    changed: bool = False
    error: str | None = None
