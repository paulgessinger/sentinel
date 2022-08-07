import pytest

from sentinel.github import rule_apply_changed_files


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
