#!/usr/bin/env python3
"""Resolve the latest stored benchmark baseline from a history tree."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--history-dir", required=True)
    parser.add_argument("--profile", required=True)
    parser.add_argument("--channel", default="main")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    history_dir = Path(args.history_dir).resolve()
    pointer_path = history_dir / "history" / args.profile / f"latest-{args.channel}.json"
    if not pointer_path.is_file():
        return 1
    pointer = json.loads(pointer_path.read_text(encoding="utf-8"))
    summary_path = history_dir / pointer["summaryPath"]
    if not summary_path.is_file():
        return 1
    print(summary_path)
    return 0


if __name__ == "__main__":
    sys.exit(main())
