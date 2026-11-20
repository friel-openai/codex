#!/usr/bin/env python3
"""Check packaged resources and the daemon fixture's socket identity."""

import hashlib
import json
import os
from pathlib import Path
import socket
import subprocess
import tempfile
import unittest
from unittest.mock import patch

import frodex_package_smoke as smoke


class PackageSmokeResourceTests(unittest.TestCase):
    """Resource checks must use only binaries included in the target package."""

    def test_resource_checks_match_package_targets(self):
        with tempfile.TemporaryDirectory(
            prefix="fdx-resource-test-", dir="/tmp"
        ) as temporary:
            root = Path(temporary)
            for target, resources in (
                (
                    "x86_64-unknown-linux-gnu",
                    {
                        "codex-path/rg": "ripgrep 1",
                        "codex-resources/bwrap": "bubblewrap 1",
                    },
                ),
                ("aarch64-apple-darwin", {"codex-path/rg": "ripgrep 1"}),
            ):
                with self.subTest(target=target):
                    package = root / target
                    package.mkdir()
                    (package / "codex-package.json").write_text(
                        json.dumps({"version": "1.2.3+frodex.0", "target": target})
                    )
                    for name in resources:
                        resource = package / name
                        resource.parent.mkdir(parents=True, exist_ok=True)
                        resource.write_text("fixture")

                    def run_resource(argv, **kwargs):
                        self.assertEqual(argv[1:], ["--version"])
                        resource = Path(argv[0])
                        name = resource.relative_to(package).as_posix()
                        self.assertIn(name, resources)
                        self.assertTrue(resource.is_file())
                        return subprocess.CompletedProcess(
                            argv, 0, resources[name], ""
                        )

                    def stop_before_bootstrap(codex, command, env, home):
                        if command == "bootstrap":
                            raise RuntimeError("resource checks complete")
                        self.assertEqual(command, "stop")
                        return {"status": "notRunning"}

                    with (
                        patch.object(
                            smoke.subprocess, "run", side_effect=run_resource
                        ) as run,
                        patch.object(
                            smoke, "daemon_command", side_effect=stop_before_bootstrap
                        ) as daemon,
                        patch.object(
                            smoke,
                            "daemon_socket_paths",
                            return_value=(root / "rendezvous", root / "protected-socket"),
                        ),
                        self.assertRaisesRegex(RuntimeError, "resource checks complete"),
                    ):
                        smoke.smoke(package, "1.2.3+frodex.0")
                    self.assertEqual(
                        [
                            Path(call.args[0][0]).relative_to(package).as_posix()
                            for call in run.call_args_list
                        ],
                        list(resources),
                    )
                    self.assertEqual(
                        [call.args[1] for call in daemon.call_args_list],
                        ["bootstrap", "stop", "stop"],
                    )
                    self.assertFalse(daemon.call_args_list[0].args[3].parent.exists())


class PackageSmokeSocketTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory(
            prefix="fdx-smoke-test-", dir="/tmp"
        )
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        self.home = self.root / "home"
        self.home.mkdir()
        self.rendezvous = self.home / "app-server-control.sock"
        self.directory = self.root / "protected"
        self.directory.mkdir(mode=0o700)
        self.protected = self.directory / "fixture-socket"
        self.listener = socket.socket(socket.AF_UNIX)
        self.addCleanup(self.listener.close)
        self.listener.bind(str(self.protected))
        self.protected.chmod(0o600)
        self.rendezvous.symlink_to(self.protected)

    def validate(self, advertised=None):
        with patch.object(
            smoke, "daemon_socket_paths", return_value=(self.rendezvous, self.protected)
        ):
            return smoke.require_daemon_socket(advertised or self.rendezvous, self.home)

    def test_exact_protected_socket_is_allowed_outside_codex_home(self):
        self.assertFalse(self.rendezvous.resolve().is_relative_to(self.home))
        self.assertEqual(self.validate(), self.protected)

    def test_other_advertised_socket_is_rejected(self):
        with self.assertRaisesRegex(RuntimeError, "advertised another"):
            self.validate(self.root / "another.sock")

    def test_alias_to_another_socket_is_rejected(self):
        self.rendezvous.unlink()
        self.rendezvous.symlink_to(self.directory / "another.sock")
        with self.assertRaisesRegex(RuntimeError, "does not name"):
            self.validate()

    def test_nonprivate_directory_is_rejected(self):
        self.directory.chmod(0o755)
        with self.assertRaisesRegex(RuntimeError, "directory is not private"):
            self.validate()

    def test_nonprivate_socket_is_rejected(self):
        self.protected.chmod(0o666)
        with self.assertRaisesRegex(RuntimeError, "socket is not private"):
            self.validate()

    def test_another_users_directory_is_rejected(self):
        with patch.object(smoke.os, "geteuid", return_value=os.geteuid() + 1):
            with self.assertRaisesRegex(RuntimeError, "directory is not private"):
                self.validate()

    def test_symlinked_protected_directory_is_rejected(self):
        moved = self.root / "moved"
        self.directory.rename(moved)
        self.directory.symlink_to(moved, target_is_directory=True)
        with self.assertRaisesRegex(RuntimeError, "directory is not private"):
            self.validate()

    def test_regular_file_is_not_a_socket(self):
        self.protected.unlink()
        self.protected.touch(mode=0o600)
        with self.assertRaisesRegex(RuntimeError, "socket is not private"):
            self.validate()

    def test_protected_path_hashes_canonical_rendezvous_not_tmpdir(self):
        alias = self.root / "home-alias"
        alias.symlink_to(self.home, target_is_directory=True)
        (self.home / "app-server-control").mkdir()
        with patch.dict(os.environ, {"TMPDIR": str(self.root / "different-tmp")}):
            rendezvous, protected = smoke.daemon_socket_paths(alias)
        canonical = self.home.resolve() / "app-server-control/app-server-control.sock"
        self.assertEqual(
            rendezvous, alias / "app-server-control/app-server-control.sock"
        )
        self.assertEqual(
            protected,
            Path("/tmp").resolve()
            / f"codex-daemon-{os.geteuid()}"
            / hashlib.sha256(os.fsencode(canonical)).hexdigest(),
        )
        self.assertNotEqual(
            smoke.daemon_socket_paths(self.root / "another-home")[1], protected
        )


if __name__ == "__main__":
    unittest.main()
