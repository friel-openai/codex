#!/usr/bin/env python3
"""Check packaged delegation defaults and active-child limits with a local model server."""

import argparse
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
import os
from pathlib import Path
import threading

from frodex_thread_resume_smoke import load_app_server_client, response_result


CHILD_MESSAGE = "FRODEX_CAPACITY_CHILD"
PROACTIVE = "Proactive multi-agent delegation is active."
EXPLICIT = "Do not spawn sub-agents unless the user"
CUSTOM = "Delegate only when explicitly requested by the user."


def message_texts(body, role):
    return [
        part.get("text", "")
        for item in body.get("input", [])
        if item.get("role") == role
        for part in item.get("content", [])
        if isinstance(part, dict)
    ]


def spawned_task(output):
    try:
        result = json.loads(output)
    except (TypeError, json.JSONDecodeError):
        return None
    if isinstance(result, dict) and isinstance(result.get("task_name"), str):
        return result["task_name"]
    return None


def run_case(client_type, codex, home, work, name, child_limit):
    """Keep children executing until all parent spawn results have been inspected."""
    home.mkdir()
    requests = []
    outputs = {}
    condition = threading.Condition()
    release_children = threading.Event()
    children_started = 0
    errors = []
    attempts = 7 if child_limit is None else child_limit + 1

    class ModelHandler(BaseHTTPRequestHandler):
        """Supply deterministic tool calls while holding child inference requests open."""

        def log_message(self, *_args):
            pass

        def do_POST(self):
            nonlocal children_started
            body = json.loads(self.rfile.read(int(self.headers["Content-Length"])))
            if self.path.endswith("/analytics/codex/turn-costs"):
                self.send_response(200)
                self.send_header("Content-Type", "application/json")
                self.end_headers()
                self.wfile.write(b'{"turns":[]}')
                return
            if self.path != "/v1/responses":
                self.send_error(404)
                return
            child = any(
                item.get("type") == "agent_message"
                and item.get("recipient", "").startswith("/root/defaults_child_")
                for item in body.get("input", [])
            )
            if child:
                with condition:
                    children_started += 1
                    condition.notify_all()
                if not release_children.wait(40):
                    errors.append("parent did not finish before child deadline")
                number = f"child-{threading.get_ident()}"
                item = None
            else:
                requests.append(body)
                number = len(requests)
                for record in body.get("input", []):
                    if record.get("type") == "function_call_output":
                        outputs[record["call_id"]] = record["output"]
                successful = sum(
                    spawned_task(value) is not None for value in outputs.values()
                )
                with condition:
                    ready = condition.wait_for(
                        lambda: children_started >= successful, timeout=10
                    )
                if not ready:
                    errors.append(
                        "spawn returned before child inference became observable"
                    )
                    self.send_error(500)
                    return
                item = (
                    {
                        "type": "function_call",
                        "call_id": f"spawn-{number}",
                        "namespace": "collaboration",
                        "name": "spawn_agent",
                        "arguments": json.dumps(
                            {
                                "task_name": f"defaults_child_{number}",
                                "message": f"{CHILD_MESSAGE} {number}",
                                "fork_turns": "none",
                            }
                        ),
                    }
                    if number <= attempts
                    else None
                )
            if item is None:
                item = {
                    "type": "message",
                    "id": f"msg-{number}",
                    "role": "assistant",
                    "content": [{"type": "output_text", "text": "DONE"}],
                }
            events = [
                {"type": "response.created", "response": {"id": f"resp-{number}"}},
                {"type": "response.output_item.done", "item": item},
                {"type": "response.completed", "response": {"id": f"resp-{number}"}},
            ]
            response = "".join(
                f"event: {event['type']}\ndata: {json.dumps(event)}\n\n"
                for event in events
            ).encode()
            self.send_response(200)
            self.send_header("Content-Type", "text/event-stream")
            self.send_header("Content-Length", str(len(response)))
            self.end_headers()
            try:
                self.wfile.write(response)
            except (BrokenPipeError, ConnectionResetError):
                if not child:
                    raise

    server = ThreadingHTTPServer(("127.0.0.1", 0), ModelHandler)
    worker = threading.Thread(target=server.serve_forever, daemon=True)
    worker.start()
    config = f"""model = "mock-model"
model_provider = "mock_provider"
model_reasoning_effort = "high"
approval_policy = "never"
sandbox_mode = "read-only"
[features]
code_mode = false
goal_supervisor = false
[model_providers.mock_provider]
name = "Agent defaults admission mock"
base_url = "http://127.0.0.1:{server.server_port}/v1"
wire_api = "responses"
request_max_retries = 0
stream_max_retries = 0
"""
    if child_limit is not None:
        config += f'''[agents]
max_concurrent_threads_per_session = {child_limit}
[features.multi_agent_v2]
multi_agent_mode_hint_text = "{CUSTOM}"
'''
    (home / "config.toml").write_text(config)
    environment = dict(os.environ, CODEX_HOME=str(home))
    try:
        with client_type(
            codex, env=environment, cwd=work, timeout_seconds=40
        ) as client:
            response_result(
                client.request(
                    "initialize",
                    {
                        "clientInfo": {
                            "name": "frodex-agent-defaults-admission",
                            "version": "1",
                        },
                        "capabilities": {"experimentalApi": True},
                    },
                ),
                "initialize",
            )
            client.notify("initialized")
            thread = response_result(
                client.request(
                    "thread/start",
                    {
                        "cwd": str(work),
                        "historyMode": "paginated",
                    },
                ),
                "thread/start",
            )["thread"]["id"]
            started = response_result(
                client.request(
                    "turn/start",
                    {
                        "threadId": thread,
                        "input": [
                            {
                                "type": "text",
                                "text": "Exercise agent defaults.",
                                "textElements": [],
                            }
                        ],
                    },
                ),
                "turn/start",
            )
            completed = client.wait_for_notification(
                "turn/completed",
                predicate=lambda params: (
                    params.get("threadId") == thread
                    and params.get("turn", {}).get("id") == started["turn"]["id"]
                ),
            )
            release_children.set()
            turn_status = completed["params"]["turn"]["status"]
    finally:
        release_children.set()
        server.shutdown()
        server.server_close()
        worker.join(timeout=5)
    successes = []
    failures = []
    for call_id, output in outputs.items():
        task = spawned_task(output)
        if task is not None:
            successes.append(task)
        else:
            failures.append({"call_id": call_id, "output": output})
    instructions = (
        "\n".join(message_texts(requests[0], "developer")) if requests else ""
    )
    expected_count = attempts if child_limit is None else child_limit
    assertions = {
        "parent_turn_completed": turn_status == "completed",
        "all_spawn_results_returned": len(outputs) == attempts,
        "distinct_children": len(set(successes)) == expected_count,
        "children_were_active": children_started == expected_count,
        "no_mock_errors": not errors,
        "capacity_guidance": f"There are {257 if child_limit is None else child_limit + 1} available concurrency slots"
        in instructions,
        "delegation_policy": (
            PROACTIVE in instructions and EXPLICIT not in instructions
            if child_limit is None
            else CUSTOM in instructions and PROACTIVE not in instructions
        ),
        "limit_enforcement": (
            not failures
            if child_limit is None
            else len(failures) == 1
            and "agent thread limit reached" in str(failures[0]["output"])
        ),
    }
    return {
        "name": name,
        "assertions": assertions,
        "children_started": children_started,
        "successes": successes,
        "failures": failures,
        "errors": errors,
        "turn_status": turn_status,
    }


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--codex", type=Path, required=True)
    parser.add_argument("--source", type=Path, required=True)
    parser.add_argument("--report", type=Path, required=True)
    args = parser.parse_args()
    client_type = load_app_server_client(args.source)
    home = Path(os.environ["CODEX_HOME"])
    cases = [
        run_case(client_type, args.codex, home / name, Path.cwd(), name, limit)
        for name, limit in [("defaults", None), ("explicit-limit-and-policy", 2)]
    ]
    passed = all(all(case["assertions"].values()) for case in cases)
    args.report.write_text(
        json.dumps(
            {
                "schema_version": 1,
                "passed": passed,
                "cases": cases,
                "assertions": [
                    f"{case['name']}:{key}"
                    for case in cases
                    for key, value in case["assertions"].items()
                    if value
                ],
            },
            indent=2,
        )
        + "\n"
    )
    if not passed:
        raise RuntimeError(f"agent defaults regression: {args.report}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
