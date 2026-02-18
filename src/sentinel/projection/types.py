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

    @property
    def key(self) -> str:
        return f"{self.repo_id}:{self.head_sha}"


@dataclass(frozen=True)
class EvaluationResult:
    result: str
    check_run_id: int | None = None
    changed: bool = False
    error: str | None = None
