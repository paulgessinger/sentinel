from unittest.mock import Mock
import pytest

from sentinel.github import determine_rules, rule_apply_changed_files
from sentinel.model import Rule


def test_path_filters():

    with pytest.raises(ValueError):
        rule_apply_changed_files(["source/file.cpp", "header/file.hpp"])

    files = ["source/file.cpp", "header/file.hpp"]
    assert not rule_apply_changed_files(files, paths=[], paths_ignore=[])

    assert rule_apply_changed_files(files, paths=["*"])
    assert not rule_apply_changed_files(files, paths=["nope/*"])

    assert rule_apply_changed_files(files, paths=["source/*"])
    assert rule_apply_changed_files(files, paths=["header/*"])

    assert rule_apply_changed_files(files, paths_ignore=[])

    assert rule_apply_changed_files(files, paths_ignore=["header/*"])
    assert rule_apply_changed_files(files, paths_ignore=["source/*"])
    assert not rule_apply_changed_files(files, paths_ignore=["header/*", "source/*"])

    assert rule_apply_changed_files(files, paths_ignore=["README.md"])
    assert not rule_apply_changed_files(["README.md"], paths_ignore=["README.md"])


def test_rule_selection_branch():
    rules = [Rule(branch_filter=[])]

    pr = Mock()
    pr.base = Mock()
    pr.base.ref = "main"

    selected_rules = determine_rules([], pr, rules)

    assert len(selected_rules) == 1
    assert selected_rules[0] == rules[0]

    rules = [
        Rule(branch_filter=[]),
        Rule(branch_filter=["main"]),
        Rule(branch_filter=["develop/*"]),
    ]

    selected_rules = determine_rules([], pr, rules)
    assert len(selected_rules) == 2
    assert selected_rules == rules[:-1]


def test_rule_selection_files():
    rules = [
        Rule(paths=["docs/*"]),
        Rule(paths_ignore=["docs/*"]),
        Rule(paths_ignore=["README.md"]),
    ]

    pr = Mock()

    selected_rules = determine_rules(["docs/index.md"], pr, rules)
    assert len(selected_rules) == 2
    assert selected_rules[0] == rules[0]
    assert selected_rules[1] == rules[2]

    selected_rules = determine_rules(["docs/index.md", "docs/api.md"], pr, rules)
    assert len(selected_rules) == 2
    assert selected_rules[0] == rules[0]
    assert selected_rules[1] == rules[2]

    selected_rules = determine_rules(
        ["docs/index.md", "docs/api.md", "src/main.cpp"], pr, rules
    )
    assert len(selected_rules) == 3
    assert selected_rules == rules[:3]

    selected_rules = determine_rules(["src/main.cpp"], pr, rules)
    assert len(selected_rules) == 2
    assert selected_rules[0] == rules[1]
    assert selected_rules[1] == rules[2]
