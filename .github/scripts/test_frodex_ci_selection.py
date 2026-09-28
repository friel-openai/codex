"""Exercise CI selection validation without building or running projected Rust code."""

import copy
import importlib.util
import json
from pathlib import Path
import subprocess
import unittest
from unittest import mock


SPEC = importlib.util.spec_from_file_location(
    "frodex_ci_selection", Path(__file__).with_name("frodex_ci_selection.py")
)
selection = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(selection)


def group(binary="codex-core", names=None, kind="lib", target=None):
    return {
        "package": "codex-core",
        "binary": binary,
        "kind": kind,
        "target": target,
        "tests": ["module::exact_case"] if names is None else names,
    }


def declaration(*groups):
    return {"schema_version": 1, "groups": list(groups)}


def case(status="matches", ignored=False):
    return {"ignored": ignored, "filter-match": {"status": status}}


def listing(binary="codex-core", cases=None):
    return {
        "rust-suites": {
            binary: {
                "testcases": {"module::exact_case": case()} if cases is None else cases,
            }
        }
    }


class DeclarationTests(unittest.TestCase):
    def test_same_test_name_in_distinct_binaries_is_valid(self):
        groups = [group(), group("codex-core::all", kind="test", target="all")]
        self.assertEqual(selection.load_selection(declaration(*groups)), groups)

    def test_rejects_duplicate_test_and_binary_declarations(self):
        values = [
            declaration(group(names=["module::exact_case", "module::exact_case"])),
            declaration(group(), group(names=["module::other_case"])),
        ]
        for value in values:
            with self.subTest(value=value), self.assertRaises(ValueError):
                selection.load_selection(value)

    def test_rejects_invalid_schemas_and_empty_groups(self):
        for value in [None, [], {}, declaration(), {"schema_version": 2, "groups": [group()]}]:
            with self.subTest(value=value), self.assertRaises(ValueError):
                selection.load_selection(value)
        value = declaration(group())
        value["unexpected"] = True
        with self.assertRaises(ValueError):
            selection.load_selection(value)

    def test_rejects_invalid_group_fields_and_filter_expressions(self):
        invalid_fields = [
            ("package", "other-package"),
            ("package", None),
            ("binary", ""),
            ("binary", "codex-core | all()"),
            ("kind", "bench"),
            ("target", "all"),
            ("tests", []),
            ("tests", "module::exact_case"),
            ("tests", [""]),
            ("tests", [None]),
            ("tests", ["exact_case) | all()"]),
        ]
        for field, value in invalid_fields:
            invalid = group()
            invalid[field] = value
            with self.subTest(field=field, value=value), self.assertRaises(ValueError):
                selection.load_selection(declaration(invalid))
        for target in [None, "", "all --lib"]:
            with self.subTest(target=target), self.assertRaises(ValueError):
                selection.load_selection(declaration(group(kind="test", target=target)))
        for invalid in [dict(group(), extra=True), {k: v for k, v in group().items() if k != "tests"}]:
            with self.subTest(group=invalid), self.assertRaises(ValueError):
                selection.load_selection(declaration(invalid))

    def test_limits_total_cases_across_groups(self):
        first = group(names=[f"module::case_{index}" for index in range(128)])
        second = group("codex-core::all", list(first["tests"]), "test", "all")
        self.assertEqual(selection.load_selection(declaration(first, second)), [first, second])
        second["tests"].append("module::extra_case")
        with self.assertRaises(ValueError):
            selection.load_selection(declaration(first, second))

    def test_rejects_duplicate_json_keys_in_declarations_and_listings(self):
        duplicate_documents = [
            '{"schema_version":1,"schema_version":1,"groups":[]}',
            '{"groups":[{"tests":[],"tests":[]}]}',
            '{"rust-suites":{"codex-core":{},"codex-core":{}}}',
            '{"rust-suites":{"codex-core":{"testcases":{"same":{},"same":{}}}}}',
        ]
        for text in duplicate_documents:
            with self.subTest(text=text), self.assertRaises(ValueError):
                selection.load_json(text)
        value = declaration(group())
        self.assertEqual(selection.load_json(json.dumps(value)), value)


class ListingTests(unittest.TestCase):
    def test_accepts_exact_case_and_unselected_ignored_cases(self):
        value = listing(cases={
            "module::exact_case": case(),
            "module::exact_case_suffix": case("mismatch", ignored=True),
        })
        self.assertIsNone(selection.checked_selection(value, group()))

    def test_rejects_missing_mismatched_ignored_and_unexpected_cases(self):
        values = [
            listing(cases={}),
            listing(cases={"module::exact_case": case("mismatch")}),
            listing(cases={"module::exact_case": case(ignored=True)}),
            listing(cases={"module::exact_case": case("mismatch", ignored=True)}),
            listing(cases={"module::exact_case_suffix": case()}),
            listing(cases={"module::exact_case": case(), "module::extra_case": case()}),
            listing("codex-core::all"),
        ]
        for value in values:
            with self.subTest(value=value), self.assertRaises(ValueError):
                selection.checked_selection(value, group())

    def test_rejects_same_named_match_from_another_binary(self):
        value = listing()
        value["rust-suites"].update(listing("codex-core::all")["rust-suites"])
        with self.assertRaises(ValueError):
            selection.checked_selection(value, group())

    def test_rejects_malformed_listing_records(self):
        values = [{}, {"rust-suites": []}, {"rust-suites": {"codex-core": {}}}]
        for invalid_case in [{}, case("unknown"), dict(case(), ignored="false")]:
            values.append(listing(cases={"module::exact_case": invalid_case}))
        for value in values:
            with self.subTest(value=value), self.assertRaises(ValueError):
                selection.checked_selection(value, group())


class ExecutionTests(unittest.TestCase):
    def setUp(self):
        self.source = Path("projection")
        self.config = Path("canonical") / ".github" / "nextest-ci.toml"
        self.groups = [
            group(names=["module::exact_case", "module::second_case"]),
            group("codex-core::all", kind="test", target="all"),
        ]
        self.arguments = [
            ["--cargo-profile", "ci-test", "--config-file", str(self.config),
             "--locked", "-p", "codex-core", "--lib", "-E",
             "test(=module::exact_case) | test(=module::second_case)"],
            ["--cargo-profile", "ci-test", "--config-file", str(self.config),
             "--locked", "-p", "codex-core", "--test", "all", "-E",
             "test(=module::exact_case)"],
        ]
        self.listings = [
            listing(cases={"module::exact_case": case(), "module::second_case": case()}),
            listing("codex-core::all"),
        ]

    def test_constructs_exact_filters_with_explicit_cargo_targets(self):
        for selected, expected in zip(self.groups, self.arguments):
            with self.subTest(binary=selected["binary"]):
                self.assertEqual(selection.command_arguments(selected, self.config), expected)

    @mock.patch.object(selection.subprocess, "run")
    def test_validates_all_listings_before_running_identical_selections(self, run):
        run.side_effect = [
            subprocess.CompletedProcess([], 0, stdout=json.dumps(value))
            for value in self.listings
        ] + [subprocess.CompletedProcess([], 0), subprocess.CompletedProcess([], 0)]
        selection.run_selected(self.source, self.config, self.groups)
        expected = [
            mock.call(
                ["cargo", "nextest", "list", "--message-format", "json", *arguments],
                cwd=self.source / "codex-rs", check=True, text=True, stdout=subprocess.PIPE,
            )
            for arguments in self.arguments
        ] + [
            mock.call(
                ["just", "--set", "rust_min_stack", "33554432", "test",
                 *arguments, "--no-tests", "fail"],
                cwd=self.source, check=True,
            )
            for arguments in self.arguments
        ]
        self.assertEqual(run.call_args_list, expected)

    @mock.patch.object(selection.subprocess, "run")
    def test_second_listing_failure_prevents_every_test_run(self, run):
        values = copy.deepcopy(self.listings)
        values[1]["rust-suites"]["codex-core::all"]["testcases"].clear()
        run.side_effect = [
            subprocess.CompletedProcess([], 0, stdout=json.dumps(value)) for value in values
        ]
        with self.assertRaises(ValueError):
            selection.run_selected(self.source, self.config, self.groups)
        self.assertEqual(len(run.call_args_list), 2)
        self.assertTrue(all(call.args[0][:3] == ["cargo", "nextest", "list"]
                            for call in run.call_args_list))


if __name__ == "__main__":
    unittest.main()
