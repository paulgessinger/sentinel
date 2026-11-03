import json
import os
from pathlib import Path
import subprocess

import pytest

SCRIPT = Path(__file__).resolve().parents[1] / "scripts" / "cluster_image.sh"
DIGEST = "sha256:" + "a" * 64
IMAGE = "registry:5000/merge-sentinel/merge-sentinel@" + DIGEST


@pytest.fixture
def build_environment(tmp_path):
    binary = tmp_path / "bin"
    binary.mkdir()
    # Fake the external commands; no cluster writes or real Git commits.
    command = """#!/usr/bin/env python3
import json, os, sys
from pathlib import Path
args = sys.argv[1:]
if Path(sys.argv[0]).name == "git":
    if args[:2] == ["rev-parse", "--show-toplevel"]:
        print(os.environ["TEST_ROOT"])
    elif args[0] == "rev-parse":
        print("b" * 40)
    elif args[0] == "diff" and os.environ.get("TEST_MODE") == "dirty":
        sys.exit(1)
    elif args[0] == "archive":
        sys.stdout.write("source archive")
else:
    with open(os.environ["TEST_LOG"], "a") as log:
        log.write(json.dumps(args) + "\\n")
    if args[0] == "start-build":
        if os.environ.get("TEST_MODE") == "build-failed":
            sys.exit(1)
        print("build.build.openshift.io/merge-sentinel-42")
    elif args[0] == "get":
        if "phase" in args[-1]:
            print("Complete")
        elif "imageDigest" in args[-1]:
            print("" if os.environ.get("TEST_MODE") == "missing-digest" else "sha256:" + "a" * 64)
        else:
            print("registry:5000/merge-sentinel/merge-sentinel:latest")
    elif args[:2] == ["set", "image"]:
        print("rendered deployment")
    elif args[0] == "apply" and args[-1] == "-":
        sys.stdin.read()
"""
    for name in ("git", "oc"):
        executable = binary / name
        executable.write_text(command)
        executable.chmod(0o755)
    return {
        **os.environ,
        "PATH": str(binary) + os.pathsep + os.environ["PATH"],
        "TEST_ROOT": str(tmp_path),
        "TEST_LOG": str(tmp_path / "commands.jsonl"),
    }


def calls(environment):
    path = Path(environment["TEST_LOG"])
    return (
        [json.loads(line) for line in path.read_text().splitlines()]
        if path.exists()
        else []
    )


def test_deploy_uses_build_digest_for_both_containers(build_environment):
    result = subprocess.run(
        ["bash", str(SCRIPT), "--deploy"],
        env=build_environment,
        capture_output=True,
        text=True,
    )
    assert result.returncode == 0, result.stderr
    assert result.stdout.strip() == IMAGE
    commands = calls(build_environment)
    update = next(command for command in commands if command[:2] == ["set", "image"])
    assert "web=" + IMAGE in update
    assert "migrate=" + IMAGE in update
    assert commands[-1][0] == "rollout"


@pytest.mark.parametrize("mode", ["dirty", "build-failed", "missing-digest"])
def test_failed_build_never_updates_runtime(build_environment, mode):
    build_environment["TEST_MODE"] = mode
    result = subprocess.run(
        ["bash", str(SCRIPT), "--deploy"],
        env=build_environment,
        capture_output=True,
        text=True,
    )
    assert result.returncode != 0
    commands = calls(build_environment)
    assert not any(command[0] in {"set", "rollout"} for command in commands)
    assert not any("deploy/deployment.yaml" in command for command in commands)
    if mode == "dirty":
        assert commands == []
