#!/usr/bin/env python3
"""Check the packaged daemon fixture's advertised and protected socket identity."""

import hashlib
import os
from pathlib import Path
import socket
import tempfile
import unittest
from unittest.mock import patch

import frodex_package_smoke as smoke


class PackageSmokeSocketTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory(prefix="fdx-smoke-test-", dir="/tmp")
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
        self.assertEqual(rendezvous, alias / "app-server-control/app-server-control.sock")
        self.assertEqual(
            protected,
            Path("/tmp").resolve()
            / f"codex-daemon-{os.geteuid()}"
            / hashlib.sha256(os.fsencode(canonical)).hexdigest(),
        )
        self.assertNotEqual(smoke.daemon_socket_paths(self.root / "another-home")[1], protected)


if __name__ == "__main__":
    unittest.main()
