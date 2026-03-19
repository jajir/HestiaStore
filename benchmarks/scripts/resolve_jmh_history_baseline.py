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


def load_expected_benchmark_labels(profile_spec_path: Path) -> list[str] | None:
    profile_spec = load_json(profile_spec_path)
    expected_benchmarks = profile_spec.get("benchmarks")
    if not isinstance(expected_benchmarks, list) or not expected_benchmarks:
        return None
    expected_labels: list[str] = []
    for expected in expected_benchmarks:
        if not isinstance(expected, dict):
            return None
        label = expected.get("label")
        if not isinstance(label, str) or not label.strip():
            return None
        expected_labels.append(label)
    return expected_labels


def has_complete_profile_coverage(summary: dict,
                                  expected_labels: list[str]) -> bool:
    benchmark_by_label = {
        benchmark.get("label"): benchmark
        for benchmark in summary.get("benchmarks", [])
    }
    for label in expected_labels:
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
        expected_labels = load_expected_benchmark_labels(profile_spec_path)
        if expected_labels is None:
            return 1
        summary = load_json(summary_path)
        if not has_complete_profile_coverage(summary, expected_labels):
            return 1
    print(summary_path)
    return 0


if __name__ == "__main__":
    sys.exit(main())
