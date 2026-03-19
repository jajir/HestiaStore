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
    parser.add_argument("--profile-spec", default="")
    return parser.parse_args()


def load_json(path: Path) -> dict:
    return json.loads(path.read_text(encoding="utf-8"))


def has_complete_profile_coverage(summary: dict, profile_spec_path: Path) -> bool:
    profile_spec = load_json(profile_spec_path)
    benchmark_by_label = {
        benchmark.get("label"): benchmark
        for benchmark in summary.get("benchmarks", [])
    }
    for expected in profile_spec.get("benchmarks", []):
        label = expected.get("label")
        benchmark = benchmark_by_label.get(label)
        if benchmark is None:
            return False
        results = benchmark.get("normalized", {}).get("results", [])
        if not results:
            return False
    return True


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
    if args.profile_spec:
        profile_spec_path = Path(args.profile_spec).resolve()
        if not profile_spec_path.is_file():
            return 1
        summary = load_json(summary_path)
        if not has_complete_profile_coverage(summary, profile_spec_path):
            return 1
    print(summary_path)
    return 0


if __name__ == "__main__":
    sys.exit(main())
