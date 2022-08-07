from typing import List, Optional, Set
import pydantic


class Model(pydantic.BaseModel):
    class Config:
        extra = "forbid"


class Rule(Model):
    branch_filter: List[str] = pydantic.Field(
        default_factory=list, alias="branch-filter"
    )
    paths: Optional[List[str]] = None
    paths_ignore: Optional[List[str]] = pydantic.Field(None, alias="paths-ignore")

    required_checks: Set[str] = pydantic.Field(
        default_factory=set, alias="required-checks"
    )
    required_pattern: List[str] = pydantic.Field(
        default_factory=list, alias="required-pattern"
    )


class Config(Model):
    rules: List[Rule] = pydantic.Field(default_factory=lambda: [Rule()])
    allow_extra_failures: bool = pydantic.Field(False, alias="allow-extra-failures")
