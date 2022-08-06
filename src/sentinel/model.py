from typing import List, Optional, Set
import pydantic


class Model(pydantic.BaseModel):
    class Config:
        extra = "forbid"


class Rule(Model):
    branch_filter: List[str] = pydantic.Field(default_factory=list)
    path_filter: List[str] = pydantic.Field(default_factory=list)

    required_checks: Set[str] = pydantic.Field(default_factory=set)
    required_pattern: List[str] = pydantic.Field(default_factory=list)


class Config(Model):
    rules: List[Rule] = pydantic.Field(default_factory=lambda: [Rule()])
    allow_extra_failures: bool = False
