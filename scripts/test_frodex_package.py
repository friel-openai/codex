import hashlib
import io
import json
import os
import subprocess
import sys
import tarfile
import tempfile
import unittest
from pathlib import Path

import frodex_package as package


VERSION = "0.157.0+frodex.0"
LINUX = "x86_64-unknown-linux-gnu"


class FrodexPackageTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.root = Path(self.temporary.name)

    def tearDown(self):
        self.temporary.cleanup()

    def entries(self, target=LINUX):
        metadata = {
            "layoutVersion": 1,
            "version": VERSION,
            "target": target,
            "variant": "codex",
            "entrypoint": "bin/codex",
            "resourcesDir": "codex-resources",
            "pathDir": "codex-path",
        }
        entries = []
        for name in sorted(package.PACKAGE_DIRS):
            info = tarfile.TarInfo(name)
            info.type = tarfile.DIRTYPE
            info.mode = 0o755
            entries.append((info, b""))
        for name in sorted(package.expected_files(target)):
            payload = (
                json.dumps(metadata).encode()
                if name == "codex-package.json"
                else name.encode()
            )
            info = tarfile.TarInfo(name)
            info.size = len(payload)
            info.mode = 0o644 if name == "codex-package.json" else 0o755
            entries.append((info, payload))
        return entries

    def archive(self, entries):
        path = self.root / "package.tar.gz"
        with tarfile.open(path, "w:gz") as archive:
            for info, payload in entries:
                archive.addfile(info, io.BytesIO(payload))
        return path

    def validate(self, entries, target=LINUX, **kwargs):
        return package.validate_archive(
            self.archive(entries), VERSION, target, **kwargs
        )

    def test_all_four_targets_preserve_complete_layout_and_pair_hashes(self):
        for target in sorted(package.TARGETS):
            with self.subTest(target=target):
                result = self.validate(self.entries(target), target)
                self.assertEqual(result["layout"], "upstream-v1")
                self.assertEqual(set(result["files"]), package.expected_files(target))
                self.assertEqual(result["metadata"]["target"], target)
                for name in package.PAIR:
                    executable = result["executables"][name]
                    self.assertEqual(executable["path"], f"bin/{name}")
                    self.assertEqual(
                        executable["sha256"],
                        hashlib.sha256(f"bin/{name}".encode()).hexdigest(),
                    )

    def test_directory_entries_may_be_omitted(self):
        self.validate([(info, data) for info, data in self.entries() if info.isfile()])

    def test_upstream_builder_archives_pass_validation(self):
        builder = Path(__file__).resolve().with_name("build_codex_package.py")
        executable = self.root / "executable"
        executable.write_bytes(b"#!/bin/sh\nexit 0\n")
        executable.chmod(0o755)
        for target in sorted(package.TARGETS):
            with self.subTest(target=target):
                archive = self.root / f"{target}.tar.gz"
                argv = [
                    sys.executable,
                    str(builder),
                    "--target",
                    target,
                    "--package-version",
                    VERSION,
                    "--package-dir",
                    str(self.root / target),
                    "--archive-output",
                    str(archive),
                    "--entrypoint-bin",
                    str(executable),
                    "--code-mode-host-bin",
                    str(executable),
                    "--rg-bin",
                    str(executable),
                    "--zsh-bin",
                    str(executable),
                ]
                if target.endswith("-linux-gnu"):
                    argv.extend(["--bwrap-bin", str(executable)])
                subprocess.run(
                    argv,
                    env={**os.environ, "CODEX_REPO_ROOT": str(builder.parent.parent)},
                    check=True,
                    capture_output=True,
                    text=True,
                )
                result = package.validate_archive(archive, VERSION, target)
                self.assertEqual(set(result["files"]), package.expected_files(target))

    def test_every_required_file_is_required(self):
        for missing in package.expected_files(LINUX):
            with (
                self.subTest(missing=missing),
                self.assertRaisesRegex(ValueError, "invalid package members"),
            ):
                self.validate(
                    [
                        (info, data)
                        for info, data in self.entries()
                        if info.name != missing
                    ]
                )

    def test_mac_rejects_linux_resource(self):
        entries = self.entries("aarch64-apple-darwin")
        info = tarfile.TarInfo("codex-resources/bwrap")
        info.size = 1
        info.mode = 0o755
        with self.assertRaisesRegex(ValueError, "unexpected"):
            self.validate(entries + [(info, b"x")], "aarch64-apple-darwin")

    def test_manifest_fields_are_exact(self):
        changes = {
            "layoutVersion": [2, True],
            "version": ["other"],
            "target": ["aarch64-apple-darwin"],
            "variant": ["codex-app-server"],
            "entrypoint": ["codex", "../codex"],
            "resourcesDir": ["resources"],
            "pathDir": ["path"],
            "extra": ["unexpected"],
        }
        for key, values in changes.items():
            for value in values:
                entries = self.entries()
                for index, (info, data) in enumerate(entries):
                    if info.name == "codex-package.json":
                        metadata = json.loads(data)
                        metadata[key] = value
                        data = json.dumps(metadata).encode()
                        info.size = len(data)
                        entries[index] = info, data
                with (
                    self.subTest(key=key, value=value),
                    self.assertRaisesRegex(ValueError, "invalid package metadata"),
                ):
                    self.validate(entries)

    def test_duplicate_manifest_key_is_rejected(self):
        entries = self.entries()
        for index, (info, data) in enumerate(entries):
            if info.name == "codex-package.json":
                data = data.replace(
                    b'"layoutVersion": 1', b'"layoutVersion": 2, "layoutVersion": 1'
                )
                info.size = len(data)
                entries[index] = info, data
        with self.assertRaisesRegex(ValueError, "duplicate package metadata key"):
            self.validate(entries)

    def test_manifest_requires_utf8_without_a_byte_order_mark(self):
        for encoding in ("utf-8-sig", "utf-16", "utf-32"):
            entries = self.entries()
            for index, (info, data) in enumerate(entries):
                if info.name == "codex-package.json":
                    data = data.decode("utf-8").encode(encoding)
                    info.size = len(data)
                    entries[index] = info, data
            with self.subTest(encoding=encoding), self.assertRaises(ValueError):
                self.validate(entries)

    def test_every_executable_requires_owner_execute_permission(self):
        for name in package.expected_files(LINUX) - {"codex-package.json"}:
            entries = self.entries()
            for info, _ in entries:
                if info.name == name:
                    info.mode = 0o644
            with (
                self.subTest(name=name),
                self.assertRaisesRegex(ValueError, "owner-executable"),
            ):
                self.validate(entries)

    def test_duplicate_members_are_rejected(self):
        entries = self.entries()
        with self.assertRaisesRegex(ValueError, "duplicate archive member"):
            self.validate(entries + [entries[-1]])

    def test_unreadable_files_and_unsearchable_directories_are_rejected(self):
        for name, mode in (("bin/codex", 0o111), ("bin", 0o644)):
            entries = self.entries()
            for info, _ in entries:
                if info.name == name:
                    info.mode = mode
            with (
                self.subTest(name=name),
                self.assertRaisesRegex(ValueError, "unreadable"),
            ):
                self.validate(entries)

    def test_links_devices_and_noncanonical_names_are_rejected(self):
        for member_type in (
            tarfile.SYMTYPE,
            tarfile.LNKTYPE,
            tarfile.CHRTYPE,
            tarfile.FIFOTYPE,
        ):
            info = tarfile.TarInfo("unexpected")
            info.type = member_type
            info.linkname = "/tmp/outside"
            with (
                self.subTest(member_type=member_type),
                self.assertRaisesRegex(ValueError, "regular file or directory"),
            ):
                self.validate(self.entries() + [(info, b"")])
        for name in (
            "../outside",
            "/outside",
            "bin/../outside",
            "./codex",
            "bin//codex",
            "bin\\codex",
        ):
            info = tarfile.TarInfo(name)
            with (
                self.subTest(name=name),
                self.assertRaisesRegex(ValueError, "noncanonical"),
            ):
                self.validate(self.entries() + [(info, b"")])

    def test_privileged_permissions_and_empty_files_are_rejected(self):
        for mode, empty, error in (
            (0o4755, False, "privileged"),
            (0o755, True, "empty package file"),
        ):
            entries = self.entries()
            for index, (info, data) in enumerate(entries):
                if info.name == "bin/codex":
                    info.mode = mode
                    if empty:
                        info.size = 0
                        entries[index] = info, b""
            with (
                self.subTest(mode=mode, empty=empty),
                self.assertRaisesRegex(ValueError, error),
            ):
                self.validate(entries)

    def test_legacy_layout_requires_explicit_opt_in(self):
        entries = []
        for name in package.PAIR:
            info = tarfile.TarInfo(name)
            info.mode = 0o755
            info.size = len(name)
            entries.append((info, name.encode()))
        with self.assertRaisesRegex(ValueError, "invalid package members"):
            self.validate(entries)
        result = self.validate(entries, allow_legacy=True)
        self.assertEqual(result["layout"], "legacy-flat")
        self.assertEqual(result["executables"]["codex"]["path"], "codex")

    def test_extraction_preserves_resources_and_refuses_existing_destination(self):
        archive = self.archive(self.entries())
        destination = self.root / "package"
        result = package.extract_archive(archive, destination, VERSION, LINUX)
        for name, identity in result["files"].items():
            path = destination / name
            self.assertEqual(
                hashlib.sha256(path.read_bytes()).hexdigest(), identity["sha256"]
            )
        self.assertEqual((destination / "bin/codex").stat().st_mode & 0o777, 0o755)
        with self.assertRaises(FileExistsError):
            package.extract_archive(archive, destination, VERSION, LINUX)
        self.assertTrue((destination / "bin/codex").is_file())

    def test_rejected_extraction_removes_partial_destination(self):
        archive = self.archive(self.entries()[:-1])
        destination = self.root / "package"
        with self.assertRaises(ValueError):
            package.extract_archive(archive, destination, VERSION, LINUX)
        self.assertFalse(destination.exists())


if __name__ == "__main__":
    unittest.main()
