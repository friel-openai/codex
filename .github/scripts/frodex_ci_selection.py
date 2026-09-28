#!/usr/bin/env python3
"""Validate exact nextest selections before running any projected-source tests."""

import argparse
import json
from pathlib import Path
import re
import subprocess
import sys


def require(condition, message):
    if not condition:
        raise ValueError(message)


def load_json(text):
    def unique_object(pairs):
        result = {}
        for key, value in pairs:
            require(key not in result, f"Duplicate JSON key: {key}")
            result[key] = value
        return result

    return json.loads(text, object_pairs_hook=unique_object)


def load_selection(value):
    """Each group names one Cargo target and its nextest binary ID, not its filename."""
    require(isinstance(value, dict) and set(value) == {"schema_version", "groups"}
            and value["schema_version"] == 1, "Unknown CI selection schema")
    groups = value["groups"]
    require(isinstance(groups, list) and groups, "CI selection requires groups")
    seen = set()
    binaries = set()
    for group in groups:
        require(isinstance(group, dict)
                and set(group) == {"package", "binary", "kind", "target", "tests"},
                "Each group requires package, binary, kind, target and tests")
        package, binary = group["package"], group["binary"]
        require(isinstance(package, str) and re.fullmatch(r"codex-[a-z0-9-]+", package),
                "Invalid Cargo package")
        require(isinstance(binary, str) and re.fullmatch(r"[A-Za-z0-9_:-]+", binary),
                "Invalid nextest binary ID")
        require(binary not in binaries, f"Duplicate binary group: {binary}")
        binaries.add(binary)
        require((group["kind"] == "lib" and group["target"] is None)
                or (group["kind"] == "test" and isinstance(group["target"], str)
                    and re.fullmatch(r"[A-Za-z0-9_-]+", group["target"])),
                "Use a library target or an explicit integration-test target")
        names = group["tests"]
        require(isinstance(names, list) and names, "Each group requires exact test names")
        for name in names:
            # Rust identifiers need no nextest expression escaping. Reject operators
            # instead of allowing a test declaration to broaden the filter.
            require(isinstance(name, str) and re.fullmatch(r"[A-Za-z0-9_:]+", name),
                    "Invalid exact test name")
            case = (binary, name)
            require(case not in seen, f"Duplicate intended case: {case}")
            seen.add(case)
    require(len(seen) <= 256, "CI selection exceeds 256 exact cases")
    return groups


def checked_selection(listing, group):
    suites = listing.get("rust-suites")
    require(isinstance(suites, dict), "Invalid nextest rust-suites")
    expected = {(group["binary"], name) for name in group["tests"]}
    actual = set()
    for binary, suite in suites.items():
        require(isinstance(suite, dict) and isinstance(suite.get("testcases"), dict),
                "Invalid nextest testcases")
        for name, case in suite["testcases"].items():
            require(isinstance(case, dict) and isinstance(case.get("ignored"), bool)
                    and isinstance(case.get("filter-match"), dict),
                    "Invalid nextest test case")
            status = case["filter-match"].get("status")
            require(status in {"matches", "mismatch"}, "Invalid nextest filter-match status")
            pair = (binary, name)
            if pair in expected or status == "matches":
                require(not case["ignored"], f"Ignored selected case: {pair}")
            if status == "matches":
                require(pair not in actual, f"Duplicate selected case: {pair}")
                actual.add(pair)
    missing, unexpected = sorted(expected - actual), sorted(actual - expected)
    require(not missing and not unexpected,
            f"Selection differs: missing={missing}; unexpected={unexpected}")


def command_arguments(group, config):
    arguments = ["--cargo-profile", "ci-test", "--config-file", str(config),
                 "--locked", "-p", group["package"]]
    if group["kind"] == "lib":
        arguments.append("--lib")
    else:
        arguments.extend(["--test", group["target"]])
    arguments.extend(["-E", " | ".join(f"test(={name})" for name in group["tests"])])
    return arguments


def run_selected(source, config, groups):
    commands = [command_arguments(group, config) for group in groups]
    # Validate every group before the first test executes. A renamed or ignored
    # case must fail CI, even when other cases still match the expression.
    for group, arguments in zip(groups, commands):
        result = subprocess.run(
            ["cargo", "nextest", "list", "--message-format", "json", *arguments],
            cwd=source / "codex-rs", check=True, text=True, stdout=subprocess.PIPE,
        )
        checked_selection(load_json(result.stdout), group)
    for arguments in commands:
        subprocess.run(
            ["just", "--set", "rust_min_stack", "33554432", "test",
             *arguments, "--no-tests", "fail"],
            cwd=source, check=True,
        )


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source", required=True, type=Path)
    parser.add_argument("--config", required=True, type=Path)
    parser.add_argument("--selection", required=True, type=Path)
    args = parser.parse_args()
    groups = load_selection(load_json(args.selection.read_text()))
    run_selected(args.source.resolve(), args.config.resolve(), groups)


if __name__ == "__main__":
    try:
        main()
    except (OSError, ValueError, subprocess.CalledProcessError) as error:
        print(f"frodex-ci selection: {error}", file=sys.stderr)
        sys.exit(1)
