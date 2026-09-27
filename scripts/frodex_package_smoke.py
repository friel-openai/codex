#!/usr/bin/env python3
"""Exercise a staged package's daemon installation without credentials or inference."""

import argparse
import hashlib
import json
import shutil
import socket
import subprocess
import sys
import tempfile
from pathlib import Path


def file_hash(path: Path) -> str:
    with path.open("rb") as stream:
        return hashlib.file_digest(stream, "sha256").hexdigest()


def daemon_command(codex: Path, command: str, env: dict[str, str], cwd: Path) -> dict:
    result = subprocess.run(
        [str(codex), "app-server", "daemon", command],
        env=env,
        cwd=cwd,
        stdin=subprocess.DEVNULL,
        capture_output=True,
        text=True,
        timeout=120,
    )
    if result.returncode:
        raise RuntimeError(f"daemon {command} failed: {result.stderr}")
    return json.loads(result.stdout)


def require(condition: bool, message: str) -> None:
    if not condition:
        raise RuntimeError(message)


def smoke(package: Path, version: str) -> dict:
    package = package.resolve(strict=True)
    codex = package / "bin/codex"
    metadata = json.loads((package / "codex-package.json").read_text())
    require(metadata["version"] == version, "staged package version mismatch")
    expected_files = {
        path.relative_to(package): file_hash(path)
        for path in package.rglob("*")
        if path.is_file()
    }
    # Short fixture paths also work with macOS's Unix socket path limit.
    root = Path(tempfile.mkdtemp(prefix="fdx-pkg-", dir="/tmp")).resolve()
    home = root / "home"
    codex_home = home / ".codex"
    temporary = root / "tmp"
    for directory in (home, codex_home, temporary):
        directory.mkdir(parents=True, exist_ok=False)
    env = {
        "PATH": "/usr/bin:/bin:/usr/sbin:/sbin",
        "HOME": str(home),
        "CODEX_HOME": str(codex_home),
        "TMPDIR": str(temporary),
        "XDG_CACHE_HOME": str(home / ".cache"),
        "XDG_CONFIG_HOME": str(home / ".config"),
        "XDG_DATA_HOME": str(home / ".local/share"),
        "XDG_STATE_HOME": str(home / ".local/state"),
        "LANG": "en_US.UTF-8",
        "HTTP_PROXY": "http://127.0.0.1:9",
        "HTTPS_PROXY": "http://127.0.0.1:9",
        "ALL_PROXY": "http://127.0.0.1:9",
        "NO_PROXY": "localhost,127.0.0.1,::1",
    }
    report = {}
    stopped = False
    try:
        resources = {
            "codex-path/rg": "ripgrep ",
            "codex-resources/zsh/bin/zsh": "zsh ",
        }
        if metadata["target"].endswith("-linux-gnu"):
            resources["codex-resources/bwrap"] = "bubblewrap "
        report["resource_versions"] = {}
        for name, prefix in resources.items():
            result = subprocess.run(
                [str(package / name), "--version"],
                env=env,
                cwd=home,
                stdin=subprocess.DEVNULL,
                capture_output=True,
                text=True,
                timeout=15,
            )
            require(
                result.returncode == 0 and result.stdout.startswith(prefix),
                f"packaged {name} cannot run on this target: {result.stderr}",
            )
            report["resource_versions"][name] = result.stdout.strip()
        bootstrap = daemon_command(codex, "bootstrap", env, home)
        report["bootstrap"] = bootstrap
        require(bootstrap["status"] == "bootstrapped", "daemon did not bootstrap")
        require(bootstrap["backend"] == "pid", "unexpected daemon backend")
        require(
            bootstrap["autoUpdateEnabled"] is False, "release smoke started an updater"
        )
        require(
            bootstrap["remoteControlEnabled"] is False,
            "release smoke enabled remote control",
        )
        managed = Path(bootstrap["managedCodexPath"]).resolve(strict=True)
        require(
            managed.is_relative_to(root),
            "managed executable escaped the private fixture",
        )
        managed_package = managed.parent.parent
        require(
            managed_package.name.startswith("local-"),
            "Frodex package was not locally pinned",
        )
        require(
            not (managed_package.parent.parent / "auto-update-version").exists(),
            "Frodex package acquired an updater-selection marker",
        )
        for name, digest in expected_files.items():
            require(
                file_hash(managed_package / name) == digest,
                f"managed package changed {name}",
            )
        socket_path = Path(bootstrap["socketPath"])
        require(
            socket_path.resolve().is_relative_to(root),
            "daemon socket escaped the private fixture",
        )
        with socket.socket(socket.AF_UNIX) as connection:
            connection.settimeout(5)
            connection.connect(str(socket_path))
        running = daemon_command(codex, "version", env, home)
        report["running"] = running
        require(
            running["status"] == "running", "daemon version did not confirm readiness"
        )
        for output in (bootstrap, running):
            for key in ("cliVersion", "appServerVersion", "managedCodexVersion"):
                require(
                    output[key] == version, f"daemon {key} mismatch: {output[key]!r}"
                )
        pid_files = list((codex_home / "app-server-daemon").glob("*.pid"))
        require(len(pid_files) == 1, "expected one fixture daemon and no updater")
        require(
            pid_files[0].name in {"daemon.pid", "app-server.pid"},
            "unexpected daemon PID file",
        )
        pid = json.loads(pid_files[0].read_text())["pid"]
        require(type(pid) is int and pid > 0, "invalid daemon PID")
        report["pid"] = pid
        report["package_files"] = {
            str(name): digest for name, digest in expected_files.items()
        }
    finally:
        try:
            report["stop"] = daemon_command(codex, "stop", env, home)
            idle = daemon_command(codex, "stop", env, home)
            require(
                idle["status"] == "notRunning", "daemon remained running after stop"
            )
            require(
                not list((codex_home / "app-server-daemon").glob("*.pid")),
                "daemon PID file remained after stop",
            )
            if "bootstrap" in report:
                with socket.socket(socket.AF_UNIX) as connection:
                    connection.settimeout(1)
                    try:
                        connection.connect(report["bootstrap"]["socketPath"])
                    except OSError:
                        pass
                    else:
                        raise RuntimeError(
                            "daemon socket remained connectable after stop"
                        )
            if "pid" in report:
                process = subprocess.run(
                    ["/bin/ps", "-p", str(report["pid"]), "-o", "stat="],
                    capture_output=True,
                    text=True,
                    check=False,
                )
                require(
                    not process.stdout.strip()
                    or process.stdout.strip().startswith("Z"),
                    "fixture daemon process remained after stop",
                )
            stopped = True
        finally:
            if stopped:
                shutil.rmtree(root)
            else:
                print(
                    f"Daemon cleanup failed; retained fixture at {root}",
                    file=sys.stderr,
                )
    report["passed"] = True
    report["fixture_removed"] = True
    return report


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--package", type=Path, required=True)
    parser.add_argument("--version", required=True)
    args = parser.parse_args()
    print(json.dumps(smoke(args.package, args.version), indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
