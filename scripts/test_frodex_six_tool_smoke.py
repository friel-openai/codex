import json
from pathlib import Path
import tempfile
import unittest

import frodex_six_tool_smoke as smoke


class RolloutCallsTest(unittest.TestCase):
    """Child history must neither duplicate nor satisfy the parent's assertions."""

    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.sessions = Path(self.temporary.name) / "sessions"
        self.sessions.mkdir()
        self.parent_records = []
        for index, name in enumerate(smoke.EXPECTED_TOOLS):
            self.parent_records.extend(
                [
                    {
                        "type": "response_item",
                        "payload": {
                            "type": "function_call",
                            "name": f"collaboration.{name}",
                            "call_id": f"call-{index}",
                            "arguments": "{}",
                        },
                    },
                    {
                        "type": "response_item",
                        "payload": {
                            "type": "function_call_output",
                            "call_id": f"call-{index}",
                            "output": "{}",
                        },
                    },
                ]
            )

    def write_rollout(self, name, thread_id, records):
        path = self.sessions / f"{name}.jsonl"
        metadata = {"type": "session_meta", "payload": {"id": thread_id}}
        path.write_text(
            "\n".join(json.dumps(record) for record in [metadata, *records]) + "\n",
            encoding="utf-8",
        )
        return path

    def test_inherited_child_calls_are_not_counted_again(self):
        parent = self.write_rollout("parent", "parent-id", self.parent_records)
        self.write_rollout("child", "child-id", self.parent_records[:4])
        self.assertEqual(
            smoke.rollout_calls(parent, "parent-id"),
            (list(smoke.EXPECTED_TOOLS), True),
        )

    def test_child_results_cannot_satisfy_missing_parent_results(self):
        parent = self.write_rollout("parent", "parent-id", self.parent_records[:-1])
        self.write_rollout("child", "child-id", self.parent_records)
        self.assertEqual(
            smoke.rollout_calls(parent, "parent-id"),
            (list(smoke.EXPECTED_TOOLS), False),
        )

    def test_extra_parent_calls_are_not_deduplicated(self):
        parent = self.write_rollout(
            "parent", "parent-id", self.parent_records + self.parent_records[:2]
        )
        self.assertEqual(
            smoke.rollout_calls(parent, "parent-id"),
            ([*smoke.EXPECTED_TOOLS, "list_agents"], True),
        )

    def test_child_cannot_supply_missing_parent_calls(self):
        parent = self.write_rollout("parent", "parent-id", self.parent_records[:2])
        self.write_rollout("child", "child-id", self.parent_records)
        self.assertEqual(
            smoke.rollout_calls(parent, "parent-id"), (["list_agents"], True)
        )

    def test_rejects_wrong_thread(self):
        child = self.write_rollout("child", "child-id", self.parent_records)
        with self.assertRaisesRegex(RuntimeError, "does not belong"):
            smoke.rollout_calls(child, "parent-id")

    def test_rejects_invalid_parent_arguments(self):
        self.parent_records[0]["payload"]["arguments"] = "[]"
        parent = self.write_rollout("parent", "parent-id", self.parent_records)
        with self.assertRaisesRegex(RuntimeError, "arguments were not canonical JSON"):
            smoke.rollout_calls(parent, "parent-id")

    def test_rejects_missing_parent_metadata(self):
        parent = self.sessions / "parent.jsonl"
        parent.write_text("", encoding="utf-8")
        with self.assertRaisesRegex(RuntimeError, "does not belong"):
            smoke.rollout_calls(parent, "parent-id")


if __name__ == "__main__":
    unittest.main()
