from __future__ import annotations

import hashlib
import io
import json
import stat
import subprocess
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

    def archive(
        self,
        names: tuple[str, ...] = admission.ARCHIVE_MEMBERS,
        *,
        modern: bool = False,
    ) -> Path:
        path = self.root / "candidate.tar.gz"
        if modern:
            names = tuple(
                sorted(
                    admission.frodex_package.expected_files("x86_64-unknown-linux-gnu")
                )
            )
        with tarfile.open(path, "w:gz") as bundle:
            for name in names:
                payload = f"#!/bin/sh\necho {name}\n".encode()
                if name == "codex-package.json":
                    payload = json.dumps(
                        {
                            "layoutVersion": 1,
                            "version": "1.2.3+frodex.0",
                            "target": "x86_64-unknown-linux-gnu",
                            "variant": "codex",
                            "entrypoint": "bin/codex",
                            "resourcesDir": "codex-resources",
                            "pathDir": "codex-path",
                        }
                    ).encode()
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
        with self.assertRaisesRegex(
            admission.AdmissionError, "invalid package members"
        ):
            admission.extract_candidate(
                archive,
                self.root / "candidate",
                expected_version="1.2.3+frodex.0",
                platform_name="linux-x86_64",
                entrypoint="codex",
            )

    def test_complete_package_extracts_resources_and_binds_manifest_entrypoint(
        self,
    ) -> None:
        archive = self.archive(modern=True)
        manifest = self.manifest(archive)
        manifest["platforms"]["linux-x86_64"]["path"] = "bin/codex"
        admission.verify_manifest(
            manifest, archive, "1.2.3+frodex.0", "a" * 40, "linux-x86_64"
        )
        destination = self.root / "candidate"
        pair = admission.extract_candidate(
            archive,
            destination,
            expected_version="1.2.3+frodex.0",
            platform_name="linux-x86_64",
            entrypoint="bin/codex",
        )
        self.assertEqual(
            pair, (destination / "bin/codex", destination / "bin/codex-code-mode-host")
        )
        self.assertTrue((destination / "codex-resources/bwrap").is_file())
        self.assertTrue((destination / "codex-resources/zsh/bin/zsh").is_file())
        self.assertTrue((destination / "codex-path/rg").is_file())

    def test_legacy_manifest_still_extracts_immutable_flat_package(self) -> None:
        destination = self.root / "candidate"
        pair = admission.extract_candidate(
            self.archive(),
            destination,
            expected_version="1.2.3+frodex.0",
            platform_name="linux-x86_64",
            entrypoint="codex",
        )
        self.assertEqual(
            pair, (destination / "codex", destination / "codex-code-mode-host")
        )

    def test_package_cannot_disagree_with_manifest_layout_or_version(self) -> None:
        archive = self.archive(modern=True)
        destination = self.root / "candidate"
        for version, entrypoint in (
            ("1.2.3+frodex.0", "codex"),
            ("wrong", "bin/codex"),
        ):
            with (
                self.subTest(version=version, entrypoint=entrypoint),
                self.assertRaises(admission.AdmissionError),
            ):
                admission.extract_candidate(
                    archive,
                    destination,
                    expected_version=version,
                    platform_name="linux-x86_64",
                    entrypoint=entrypoint,
                )
            self.assertFalse(destination.exists())

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

    def test_owner_inventory_rejects_stale_queue_cutoff(self) -> None:
        queue_dir = self.root / "queued"
        queue_dir.mkdir()
        (queue_dir / "feature-a.md").write_text("# A\n")
        path = self.root / "owners.json"
        path.write_text(
            json.dumps(
                {
                    "schema_version": 1,
                    "candidate_source": "a" * 40,
                    "queue_cutoff": {
                        "plans": ["feature-b.md"],
                        "sha256": admission.queue_plan_digest(["feature-b.md"]),
                    },
                    "owners": [{"id": "historical", "required": False}],
                }
            )
        )
        with self.assertRaisesRegex(admission.AdmissionError, "queue cutoff is stale"):
            admission.verify_owner_inventory(path, "a" * 40, queue_dir)

    def test_owner_inventory_accepts_exact_queue_cutoff(self) -> None:
        queue_dir = self.root / "queued"
        queue_dir.mkdir()
        (queue_dir / "feature-b-record.md").write_text("# Record\n")
        (queue_dir / "feature-b.md").write_text("# B\n")
        (queue_dir / "feature-a.md").write_text("# A\n")
        plans = ["feature-a.md", "feature-b.md"]
        path = self.root / "owners.json"
        path.write_text(
            json.dumps(
                {
                    "schema_version": 1,
                    "candidate_source": "a" * 40,
                    "queue_cutoff": {
                        "plans": plans,
                        "sha256": admission.queue_plan_digest(plans),
                    },
                    "owners": [{"id": "historical", "required": False}],
                }
            )
        )
        admission.verify_owner_inventory(path, "a" * 40, queue_dir)

    def test_six_tool_report_requires_exact_order_and_thread_resume(self) -> None:
        report = self.root / "report.json"
        report.write_text(
            json.dumps(
                {
                    "schema_version": 1,
                    "passed": True,
                    "tools": list(admission.SIX_TOOL_SEQUENCE),
                    "thread_resume": True,
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

    def test_thread_resume_report_requires_restart_sentinel_and_fixture(self) -> None:
        report = self.root / "report.json"
        payload = {
            "schema_version": 1,
            "passed": True,
            "thread_resume": True,
            "restart_thread_resume": True,
            "model_context_sentinel": admission.THREAD_RESUME_SENTINEL,
            "fixture_sha256": admission.THREAD_RESUME_FIXTURE_SHA256,
        }
        report.write_text(json.dumps(payload))
        evidence = admission.validate_case_report(report, "thread_resume")
        self.assertTrue(evidence["restart_thread_resume"])
        payload["fixture_sha256"] = "0" * 64
        report.write_text(json.dumps(payload))
        with self.assertRaisesRegex(admission.AdmissionError, "unrecognized fixture"):
            admission.validate_case_report(report, "thread_resume")

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

    def test_auth_literals_exclude_only_known_metadata_fields(self) -> None:
        path = self.root / "auth.json"
        credentials = {
            "OPENAI_API_KEY": "fixture-api-key",
            "tokens": {
                "access_token": "fixture-access-token",
                "account_id": "fixture-account",
            },
            "agent_identity": {
                "plan_type": "pro",
                "agent_private_key": "fixture-private-key",
            },
            "unknown": {
                "auth_mode": "fixture-nested-mode",
                "last_refresh": "fixture-nested-refresh",
            },
            "extra": [{"plan_type": "fixture-nested-plan"}],
        }
        path.write_text(
            json.dumps(
                {
                    "auth_mode": "chatgpt",
                    "last_refresh": "2026-10-01T00:00:00Z",
                    **credentials,
                }
            )
        )
        self.assertEqual(
            admission.auth_literals(path),
            {
                b"fixture-api-key",
                b"fixture-access-token",
                b"fixture-account",
                b"fixture-private-key",
                b"fixture-nested-mode",
                b"fixture-nested-refresh",
                b"fixture-nested-plan",
            },
        )

    def test_secret_scan_rejects_bearer_material(self) -> None:
        evidence = self.root / "evidence"
        evidence.mkdir()
        (evidence / "cases.json").write_text(
            '{"Authorization":"Bearer abcdefghijklmnop"}'
        )
        with self.assertRaisesRegex(admission.AdmissionError, "secret pattern"):
            admission.secret_scan(evidence)
        self.assertNotIn("abcdefghijklmnop", (evidence / "cases.json").read_text())

    def candidate(self) -> admission.Candidate:
        return admission.Candidate(
            version="1.2.3+frodex.0",
            source_commit="a" * 40,
            platform_name="linux-x86_64",
            archive_digest="b" * 64,
            archive_size=1,
            root=self.root / "candidate",
            codex=self.root / "candidate/codex",
            code_mode_host=self.root / "candidate/codex-code-mode-host",
            codex_digest="c" * 64,
            code_mode_host_digest="d" * 64,
        )

    def case(self, **overrides) -> dict[str, object]:
        return {
            "name": "fixture",
            "argv": ["fixture-command"],
            "timeout_seconds": 1,
            **overrides,
        }

    @mock.patch("frodex_admission.subprocess.Popen")
    def test_failed_case_retains_redacted_output_and_refreshed_auth(
        self, popen: mock.Mock
    ) -> None:
        original = "fixture-original-opaque-token"
        refreshed = "fixture-refreshed-opaque-token"
        api_key = "fixture-api-key-without-known-prefix"
        auth = self.root / "auth.json"
        auth.write_text(json.dumps({"tokens": {"access_token": original}}))
        process = popen.return_value
        process.returncode = 7

        def communicate(**kwargs):
            copied = self.root / "cases/fixture/home/auth.json"
            copied.write_text(json.dumps({"tokens": {"access_token": refreshed}}))
            return (
                f"{original} {refreshed} {api_key}\nuseful stdout\n".encode(),
                b"Traceback: useful failure\nBearer abcdefghijklmnop\n",
            )

        process.communicate.side_effect = communicate
        result = admission.run_case(
            self.case(auth=True, network=True, env={"OPENAI_API_KEY": api_key}),
            self.candidate(),
            self.root,
            self.root,
            auth,
            None,
        )
        self.assertFalse(result["passed"])
        self.assertEqual(result["exit_status"], 7)
        failure = self.root / "evidence" / result["diagnostics"]
        retained = failure.read_text()
        for literal in (original, refreshed, api_key, "abcdefghijklmnop"):
            self.assertNotIn(literal, retained)
        self.assertIn("useful stdout", retained)
        self.assertIn("Traceback: useful failure", retained)
        self.assertFalse((self.root / "cases/fixture/home/auth.json").exists())
        self.assertEqual(stat.S_IMODE(failure.stat().st_mode), 0o600)
        admission.secret_scan(failure.parent)

    def test_redaction_precedes_truncation(self) -> None:
        token = b"fixture-secret-spanning-the-byte-limit"
        payload = b"x" * (admission.DIAGNOSTIC_BYTES - 3) + token + b"more output"
        result = admission.diagnostic_text(payload, {token})
        self.assertTrue(result["truncated"])
        self.assertLessEqual(len(result["text"].encode()), admission.DIAGNOSTIC_BYTES)
        self.assertNotIn("fix", result["text"])

    def test_multiline_credential_field_withholds_stream(self) -> None:
        result = admission.diagnostic_text(
            b'{"access_token":\n"opaque-fixture-value"}', set()
        )
        self.assertEqual(
            result,
            {"text": "[diagnostics withheld: credential field]", "truncated": False},
        )

    def test_invalid_utf8_stays_within_diagnostic_byte_limit(self) -> None:
        result = admission.diagnostic_text(b"\xff" * admission.DIAGNOSTIC_BYTES, set())
        self.assertTrue(result["truncated"])
        self.assertLessEqual(len(result["text"].encode()), admission.DIAGNOSTIC_BYTES)

    @mock.patch("frodex_admission.subprocess.Popen")
    def test_successful_case_preserves_result_schema(self, popen: mock.Mock) -> None:
        popen.return_value.returncode = 0
        popen.return_value.communicate.return_value = (b"passed", b"")
        with mock.patch.object(admission.time, "monotonic", side_effect=[1, 1.25]):
            result = admission.run_case(
                self.case(),
                self.candidate(),
                self.root,
                self.root,
                None,
                None,
            )
        self.assertEqual(
            result,
            {
                "name": "fixture",
                "passed": True,
                "elapsed_ms": 250,
                "timeout_seconds": 1,
                "exit_status": 0,
                "stdout_sha256": hashlib.sha256(b"passed").hexdigest(),
                "stderr_sha256": hashlib.sha256(b"").hexdigest(),
                "stdout_bytes": 6,
                "stderr_bytes": 0,
                "network": False,
                "isolation_profile": "host-process-group",
                "auth": None,
                "report": None,
            },
        )
        self.assertFalse((self.root / "evidence").exists())

    def test_secret_scan_removes_symlink_without_rewriting_target(self) -> None:
        target = self.root / "target"
        target.write_text("must remain unchanged")
        evidence = self.root / "evidence"
        evidence.mkdir()
        (evidence / "linked.json").symlink_to(target)
        with self.assertRaises(admission.AdmissionError):
            admission.secret_scan(evidence)
        self.assertEqual(target.read_text(), "must remain unchanged")
        self.assertFalse((evidence / "linked.json").is_symlink())

    def test_secret_scan_redacts_literal_credentials_and_preserves_failure_json(
        self,
    ) -> None:
        evidence = self.root / "evidence"
        evidence.mkdir()
        admission.write_json(
            evidence / "cases.json", {"assertions": ["opaque-fixture-value"]}
        )
        with self.assertRaises(admission.AdmissionError):
            admission.secret_scan(evidence, {b"opaque-fixture-value"})
        self.assertEqual(
            admission.load_json(evidence / "cases.json"),
            {
                "schema_version": 1,
                "passed": False,
                "redacted": True,
            },
        )

    @mock.patch("frodex_admission.subprocess.Popen")
    def test_report_validation_failure_retains_redacted_exception(
        self, popen: mock.Mock
    ) -> None:
        api_key = "fixture-api-key-for-error"
        popen.return_value.returncode = 0
        popen.return_value.communicate.return_value = (b"before validation", b"")
        with mock.patch.object(
            admission,
            "validate_case_report",
            side_effect=ValueError(f"bad report {api_key}"),
        ):
            result = admission.run_case(
                self.case(env={"OPENAI_API_KEY": api_key}),
                self.candidate(),
                self.root,
                self.root,
                None,
                None,
            )
        self.assertFalse(result["passed"])
        self.assertEqual(result["exit_status"], 0)
        diagnostics = admission.load_json(
            self.root / "evidence" / result["diagnostics"]
        )
        self.assertEqual(
            diagnostics["error"], {"text": "bad report [REDACTED]", "truncated": False}
        )
        self.assertEqual(diagnostics["error_type"], "ValueError")

    @mock.patch("frodex_admission.os.killpg")
    @mock.patch("frodex_admission.subprocess.Popen")
    def test_timeout_retains_output(self, popen: mock.Mock, killpg: mock.Mock) -> None:
        process = popen.return_value
        process.returncode = -9
        process.pid = 12345
        process.communicate.side_effect = [
            subprocess.TimeoutExpired("fixture-command", 1),
            subprocess.TimeoutExpired("fixture-command", 2),
            (b"partial output", b"partial traceback"),
        ]
        result = admission.run_case(
            self.case(),
            self.candidate(),
            self.root,
            self.root,
            None,
            None,
        )
        self.assertTrue(result["timed_out"])
        self.assertFalse(result["passed"])
        self.assertEqual(killpg.call_count, 2)
        diagnostics = admission.load_json(
            self.root / "evidence" / result["diagnostics"]
        )
        self.assertEqual(diagnostics["stdout"]["text"], "partial output")

    @mock.patch("frodex_admission.os.killpg")
    @mock.patch("frodex_admission.subprocess.Popen")
    def test_communication_error_terminates_owned_process(
        self, popen: mock.Mock, killpg: mock.Mock
    ) -> None:
        process = popen.return_value
        process.pid = 12345
        process.returncode = -9
        process.poll.return_value = None
        process.communicate.side_effect = [
            OSError("fixture pipe failure"),
            (b"", b"stopped"),
        ]
        result = admission.run_case(
            self.case(),
            self.candidate(),
            self.root,
            self.root,
            None,
            None,
        )
        self.assertFalse(result["passed"])
        killpg.assert_called_once_with(process.pid, admission.signal.SIGKILL)
        diagnostics = admission.load_json(
            self.root / "evidence" / result["diagnostics"]
        )
        self.assertEqual(diagnostics["stderr"]["text"], "stopped")
        self.assertEqual(diagnostics["error"]["text"], "fixture pipe failure")

    @mock.patch("frodex_admission.subprocess.Popen")
    def test_unreadable_refreshed_auth_withholds_diagnostics(
        self, popen: mock.Mock
    ) -> None:
        auth = self.root / "auth.json"
        auth.write_text('{"fixture": "original"}')
        popen.return_value.returncode = 1

        def communicate(**kwargs):
            (self.root / "cases/fixture/home/auth.json").write_text("not JSON")
            return b"unknown-refreshed-credential", b"unknown-refreshed-credential"

        popen.return_value.communicate.side_effect = communicate
        result = admission.run_case(
            self.case(auth=True, network=True),
            self.candidate(),
            self.root,
            self.root,
            auth,
            None,
        )
        self.assertFalse(result["passed"])
        retained = (self.root / "evidence" / result["diagnostics"]).read_text()
        self.assertNotIn("unknown-refreshed-credential", retained)
        self.assertIn("diagnostics withheld", retained)
        self.assertFalse((self.root / "cases/fixture/home/auth.json").exists())

    @mock.patch("frodex_admission.subprocess.Popen")
    def test_removed_auth_withholds_diagnostics(self, popen: mock.Mock) -> None:
        auth = self.root / "auth.json"
        auth.write_text('{"fixture": "original"}')
        popen.return_value.returncode = 1

        def communicate(**kwargs):
            (self.root / "cases/fixture/home/auth.json").unlink()
            return b"unknown-refreshed-credential", b"unknown-refreshed-credential"

        popen.return_value.communicate.side_effect = communicate
        result = admission.run_case(
            self.case(auth=True, network=True),
            self.candidate(),
            self.root,
            self.root,
            auth,
            None,
        )
        self.assertFalse(result["passed"])
        retained = (self.root / "evidence" / result["diagnostics"]).read_text()
        self.assertNotIn("unknown-refreshed-credential", retained)
        self.assertIn("diagnostics withheld", retained)

    @mock.patch("frodex_admission.subprocess.Popen")
    def test_successful_report_cannot_retain_literal_credentials(
        self, popen: mock.Mock
    ) -> None:
        token = "fixture-api-key-in-report"
        popen.return_value.returncode = 0
        popen.return_value.communicate.return_value = (b"", b"")
        with mock.patch.object(
            admission, "validate_case_report", return_value={"assertions": [token]}
        ):
            result = admission.run_case(
                self.case(env={"OPENAI_API_KEY": token}),
                self.candidate(),
                self.root,
                self.root,
                None,
                None,
            )
        self.assertFalse(result["passed"])
        self.assertIsNone(result["report"])
        self.assertNotIn(token, json.dumps(result))

    @mock.patch("frodex_admission.subprocess.Popen")
    def test_case_results_survive_failure_and_runtime_cleanup(
        self, popen: mock.Mock
    ) -> None:
        archive = self.archive()
        manifest = self.root / "manifest.json"
        manifest.write_text(json.dumps(self.manifest(archive)))
        owners = self.root / "owners.json"
        owners.write_text("{}")
        cases = self.root / "case-manifest.json"
        cases.write_text(
            json.dumps(
                {
                    "schema_version": 1,
                    "cases": [
                        self.case(name=name) for name in ("first", "second", "third")
                    ],
                }
            )
        )
        run_root = self.root / "run"
        run_root.mkdir()
        first = mock.Mock(returncode=0)
        first.communicate.return_value = (b"first passed", b"")
        second = mock.Mock(returncode=1)
        second.communicate.return_value = (b"second failed", b"failure traceback")
        popen.side_effect = [first, second]
        args = admission.build_parser().parse_args(
            [
                "--manifest",
                str(manifest),
                "--archive",
                str(archive),
                "--source-repo",
                str(self.root),
                "--expected-source",
                "a" * 40,
                "--expected-version",
                "1.2.3+frodex.0",
                "--owner-inventory",
                str(owners),
                "--queue-dir",
                str(self.root),
                "--cases",
                str(cases),
                "--platform",
                "linux-x86_64",
                "--no-bwrap",
            ]
        )
        snapshots = []
        write_json = admission.write_json

        def record_write(path, payload):
            write_json(path, payload)
            if path.name == "cases.json":
                snapshots.append(admission.load_json(path))

        with (
            mock.patch.object(admission, "verify_source_checkout"),
            mock.patch.object(admission, "verify_candidate_version"),
            mock.patch.object(admission, "verify_owner_inventory", return_value={}),
            mock.patch.object(
                admission, "ensure_admission_root", return_value=run_root
            ),
            mock.patch.object(admission, "write_json", side_effect=record_write),
            mock.patch.object(
                admission, "secret_scan", wraps=admission.secret_scan
            ) as scan,
            self.assertRaisesRegex(admission.AdmissionError, "retained evidence"),
        ):
            admission.admit(args)
        self.assertEqual([len(snapshot["cases"]) for snapshot in snapshots], [0, 1, 2])
        self.assertTrue(all(snapshot["passed"] is False for snapshot in snapshots))
        self.assertEqual(
            [case["passed"] for case in snapshots[-1]["cases"]], [True, False]
        )
        self.assertFalse((run_root / "cases").exists())
        self.assertFalse((run_root / "candidate").exists())
        self.assertTrue((run_root / "evidence/second.failure.json").exists())
        cleanup = admission.load_json(run_root / "evidence/cleanup.json")
        self.assertEqual(cleanup["removed"], ["cases", "candidate"])
        scan.assert_called_once_with(run_root / "evidence", set())
        self.assertEqual(popen.call_count, 2)

    @mock.patch("frodex_admission.subprocess.Popen")
    def test_authenticated_admission_preserves_chatgpt_owner_inventory(
        self, popen: mock.Mock
    ) -> None:
        archive = self.archive()
        manifest = self.root / "manifest.json"
        manifest.write_text(json.dumps(self.manifest(archive)))
        queue = self.root / "queued"
        queue.mkdir()
        (queue / "fixture.md").write_text("# Fixture\n")
        inventory = {
            "schema_version": 1,
            "candidate_source": "a" * 40,
            "queue_cutoff": {
                "plans": ["fixture.md"],
                "sha256": admission.queue_plan_digest(["fixture.md"]),
            },
            "owners": [
                {"id": "frodex-chatgpt-responses-version-header", "required": False},
                {
                    "id": "strip-build-metadata-from-chatgpt-client-version",
                    "required": False,
                },
            ],
        }
        owners = self.root / "owners.json"
        owners.write_text(json.dumps(inventory))
        cases = self.root / "case-manifest.json"
        cases.write_text(
            json.dumps(
                {
                    "schema_version": 1,
                    "cases": [
                        self.case(auth=True, network=True, report_schema="assertions")
                    ],
                }
            )
        )
        auth = self.root / "fixture-auth.json"
        auth.write_text(
            json.dumps(
                {
                    "auth_mode": "chatgpt",
                    "last_refresh": "2026-10-01T00:00:00Z",
                    "OPENAI_API_KEY": None,
                    "tokens": {
                        "access_token": "fixture-original-access",
                        "refresh_token": "fixture-original-refresh",
                    },
                }
            )
        )
        run_root = self.root / "run"
        run_root.mkdir()
        process = popen.return_value
        process.returncode = 0
        report = {
            "schema_version": 1,
            "passed": True,
            "assertions": ["chatgpt-client-version"],
        }

        def communicate(**kwargs):
            copied = run_root / "cases/fixture/home/auth.json"
            payload = json.loads(copied.read_text())
            payload["tokens"]["access_token"] = "fixture-refreshed-access"
            copied.write_text(json.dumps(payload))
            (run_root / "cases/fixture/report.json").write_text(json.dumps(report))
            return b"chatgpt fixture passed", b""

        process.communicate.side_effect = communicate
        args = admission.build_parser().parse_args(
            [
                "--manifest",
                str(manifest),
                "--archive",
                str(archive),
                "--source-repo",
                str(self.root),
                "--expected-source",
                "a" * 40,
                "--expected-version",
                "1.2.3+frodex.0",
                "--owner-inventory",
                str(owners),
                "--queue-dir",
                str(queue),
                "--cases",
                str(cases),
                "--auth-source",
                str(auth),
                "--platform",
                "linux-x86_64",
                "--no-bwrap",
            ]
        )
        with (
            mock.patch.object(admission, "verify_source_checkout"),
            mock.patch.object(admission, "verify_candidate_version"),
            mock.patch.object(
                admission, "ensure_admission_root", return_value=run_root
            ),
            mock.patch.object(
                admission, "secret_scan", wraps=admission.secret_scan
            ) as scan,
            mock.patch("sys.stdout", new_callable=io.StringIO),
        ):
            self.assertEqual(admission.admit(args), 0)
        self.assertEqual(
            admission.load_json(run_root / "evidence/owners.json"), inventory
        )
        results = admission.load_json(run_root / "evidence/cases.json")
        self.assertTrue(results["passed"])
        self.assertEqual(results["cases"][0]["report"], report)
        self.assertFalse((run_root / "cases").exists())
        self.assertFalse((run_root / "candidate").exists())
        scan.assert_called_once_with(
            run_root / "evidence",
            {
                b"fixture-original-access",
                b"fixture-original-refresh",
                b"fixture-refreshed-access",
            },
        )

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
