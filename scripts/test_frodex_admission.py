from __future__ import annotations

import hashlib
import io
import json
import stat
import tarfile
import tempfile
import unittest
from unittest import mock
from pathlib import Path

import frodex_admission as admission


class FrodexAdmissionTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.root = Path(self.temporary.name)

    def tearDown(self) -> None:
        self.temporary.cleanup()

    def archive(self, names: tuple[str, ...] = admission.ARCHIVE_MEMBERS) -> Path:
        path = self.root / "candidate.tar.gz"
        with tarfile.open(path, "w:gz") as bundle:
            for name in names:
                payload = f"#!/bin/sh\necho {name}\n".encode()
                info = tarfile.TarInfo(name)
                info.size = len(payload)
                info.mode = 0o755
                bundle.addfile(info, io.BytesIO(payload))
        return path

    def manifest(self, archive: Path) -> dict[str, object]:
        return {
            "platforms": {
                "linux-x86_64": {
                    "size": archive.stat().st_size,
                    "hash": "sha256",
                    "digest": admission.sha256_file(archive),
                    "path": "codex",
                    "format": "tar.gz",
                }
            },
            "metadata": {
                "build-info": {"commit": {"hash": "a" * 40}},
                "release": {
                    "version": "1.2.3+frodex.0",
                    "version-output": "codex-cli 1.2.3+frodex.0",
                },
            },
        }

    def test_manifest_binds_source_version_and_archive(self) -> None:
        archive = self.archive()
        digest, size = admission.verify_manifest(
            self.manifest(archive),
            archive,
            "1.2.3+frodex.0",
            "a" * 40,
            "linux-x86_64",
        )
        self.assertEqual(digest, hashlib.sha256(archive.read_bytes()).hexdigest())
        self.assertEqual(size, archive.stat().st_size)

    def test_archive_requires_both_executable_siblings(self) -> None:
        archive = self.archive(("codex",))
        with self.assertRaisesRegex(admission.AdmissionError, "required members"):
            admission.extract_candidate(archive, self.root / "candidate")

    def test_owner_inventory_rejects_unresolved_required_owner(self) -> None:
        path = self.root / "owners.json"
        path.write_text(
            json.dumps(
                {
                    "schema_version": 1,
                    "candidate_source": "a" * 40,
                    "owners": [{"id": "shared-mcp", "required": True}],
                }
            )
        )
        with self.assertRaisesRegex(admission.AdmissionError, "unresolved"):
            admission.verify_owner_inventory(path, "a" * 40)

    def test_owner_inventory_accepts_explicit_deferral(self) -> None:
        path = self.root / "owners.json"
        path.write_text(
            json.dumps(
                {
                    "schema_version": 1,
                    "candidate_source": "a" * 40,
                    "owners": [
                        {
                            "id": "approval-tracker",
                            "required": True,
                            "resolution": "deferred",
                            "user_decision": "decision-123",
                        }
                    ],
                }
            )
        )
        admission.verify_owner_inventory(path, "a" * 40)

    def test_six_tool_report_requires_exact_order_and_history_reopen(self) -> None:
        report = self.root / "report.json"
        report.write_text(
            json.dumps(
                {
                    "schema_version": 1,
                    "passed": True,
                    "tools": list(admission.SIX_TOOL_SEQUENCE),
                    "history_reopen": True,
                    "canonical_results": True,
                }
            )
        )
        evidence = admission.validate_case_report(report, "six_tool_smoke")
        self.assertEqual(tuple(evidence["tools"]), admission.SIX_TOOL_SEQUENCE)
        payload = json.loads(report.read_text())
        payload["tools"].reverse()
        report.write_text(json.dumps(payload))
        with self.assertRaisesRegex(
            admission.AdmissionError, "exact six-tool sequence"
        ):
            admission.validate_case_report(report, "six_tool_smoke")

    def test_auth_copy_rejects_symlink_and_uses_mode_0600(self) -> None:
        source = self.root / "auth.json"
        source.write_text("synthetic")
        destination = self.root / "home" / "auth.json"
        evidence = admission.copy_auth(source, destination)
        self.assertEqual(stat.S_IMODE(destination.stat().st_mode), 0o600)
        self.assertEqual(evidence, {"basename": "auth.json", "size": 9, "mode": "0600"})
        link = self.root / "auth-link.json"
        link.symlink_to(source)
        with self.assertRaisesRegex(admission.AdmissionError, "non-symlink"):
            admission.copy_auth(link, self.root / "other-auth.json")

    def test_secret_scan_rejects_bearer_material(self) -> None:
        evidence = self.root / "evidence"
        evidence.mkdir()
        (evidence / "cases.json").write_text(
            '{"Authorization":"Bearer abcdefghijklmnop"}'
        )
        with self.assertRaisesRegex(admission.AdmissionError, "secret pattern"):
            admission.secret_scan(evidence)

    @mock.patch("frodex_admission.shutil.which", return_value="/usr/bin/bwrap")
    def test_bwrap_profiles_make_pid_isolation_explicit(
        self, _which: mock.Mock
    ) -> None:
        paths = admission.CasePaths(
            root=self.root / "case",
            home=self.root / "case/home",
            work=self.root / "case/work",
            temporary=self.root / "case/tmp",
            cache=self.root / "case/cache",
            report=self.root / "case/report.json",
        )
        full = admission.bwrap_command(
            ["/usr/bin/true"], self.root, paths, False, "full"
        )
        reduced = admission.bwrap_command(
            ["/usr/bin/true"], self.root, paths, False, "reduced"
        )

        self.assertIn("--unshare-pid", full)
        self.assertIn("--proc", full)
        self.assertNotIn("--unshare-pid", reduced)
        self.assertNotIn("--proc", reduced)
        self.assertIn("--unshare-net", full)
        self.assertIn("--unshare-net", reduced)


if __name__ == "__main__":
    unittest.main()
