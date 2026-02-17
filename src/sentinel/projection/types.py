from __future__ import annotations

from dataclasses import dataclass
from typing import Optional


@dataclass(frozen=True)
class ProjectionTrigger:
    repo_id: int
    repo_full_name: str
    head_sha: str
    installation_id: int
    delivery_id: Optional[str] = None
    event: Optional[str] = None

    @property
    def key(self) -> str:
        return f"{self.repo_id}:{self.head_sha}"


@dataclass(frozen=True)
class EvaluationResult:
    result: str
    check_run_id: Optional[int] = None
    changed: bool = False
    error: Optional[str] = None
