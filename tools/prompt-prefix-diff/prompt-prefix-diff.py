#!/usr/bin/env python3
"""Compare two prompt/token dump files and report shared-prefix metrics."""

from __future__ import annotations

import argparse
from pathlib import Path


def common_prefix_len(left: str, right: str) -> int:
    limit = min(len(left), len(right))
    for index in range(limit):
        if left[index] != right[index]:
            return index
    return limit


def line_col(text: str, offset: int) -> tuple[int, int]:
    line = text.count("\n", 0, offset) + 1
    last_newline = text.rfind("\n", 0, offset)
    column = offset + 1 if last_newline < 0 else offset - last_newline
    return line, column


def snippet(text: str, offset: int, context: int) -> str:
    start = max(0, offset - context)
    end = min(len(text), offset + context)
    return text[start:end].replace("\n", "\\n")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("left", type=Path)
    parser.add_argument("right", type=Path)
    parser.add_argument("--context", type=int, default=240)
    args = parser.parse_args()

    left = args.left.read_text(encoding="utf-8", errors="replace")
    right = args.right.read_text(encoding="utf-8", errors="replace")
    prefix = common_prefix_len(left, right)
    left_line, left_col = line_col(left, prefix)
    right_line, right_col = line_col(right, prefix)
    max_len = max(len(left), len(right), 1)
    min_len = max(min(len(left), len(right)), 1)

    print(f"left_bytes={len(left.encode('utf-8'))}")
    print(f"right_bytes={len(right.encode('utf-8'))}")
    print(f"shared_prefix_chars={prefix}")
    print(f"shared_prefix_of_shorter={prefix / min_len:.4%}")
    print(f"shared_prefix_of_longer={prefix / max_len:.4%}")
    print(f"left_first_diff_line={left_line}")
    print(f"left_first_diff_col={left_col}")
    print(f"right_first_diff_line={right_line}")
    print(f"right_first_diff_col={right_col}")
    print("left_snippet=" + snippet(left, prefix, args.context))
    print("right_snippet=" + snippet(right, prefix, args.context))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
