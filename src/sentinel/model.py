from typing import List, Optional, Set

import pydantic
from pydantic import ConfigDict


class Model(pydantic.BaseModel):
    model_config = ConfigDict(extra="forbid")


class Rule(Model):
    branch_filter: List[str] = pydantic.Field(default_factory=list)
    paths: Optional[List[str]] = None
    paths_ignore: Optional[List[str]] = None

    required_checks: Set[str] = pydantic.Field(default_factory=set)
    required_pattern: List[str] = pydantic.Field(default_factory=list)


class Config(Model):
    rules: List[Rule] = pydantic.Field(default_factory=lambda: [Rule()])
    allow_extra_failures: bool = False
