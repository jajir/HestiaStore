#!/usr/bin/env python3
"""Filter a benchmark profile down to an explicit ordered label subset."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--profile", required=True)
    parser.add_argument("--labels", required=True,
                        help="Comma-separated benchmark labels to retain.")
    parser.add_argument("--output", required=True)
    return parser.parse_args()


def load_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def main() -> int:
    args = parse_args()
    profile_path = Path(args.profile).resolve()
    output_path = Path(args.output).resolve()
    profile = load_json(profile_path)
    requested_labels = [
        label.strip()
        for label in args.labels.split(",")
        if label.strip()
    ]
    if not requested_labels:
        raise ValueError("At least one benchmark label is required.")

    benchmarks = profile.get("benchmarks")
    if not isinstance(benchmarks, list):
        raise ValueError("Profile is missing benchmark entries.")

    requested_label_set = set(requested_labels)
    filtered = [
        benchmark
        for benchmark in benchmarks
        if benchmark.get("label") in requested_label_set
    ]
    filtered_labels = {
        str(benchmark.get("label"))
        for benchmark in filtered
    }
    missing_labels = [
        label
        for label in requested_labels
        if label not in filtered_labels
    ]
    if missing_labels:
        raise ValueError(
            "Unable to resolve benchmark labels: "
            + ", ".join(missing_labels)
        )

    filtered_profile = dict(profile)
    filtered_profile["benchmarks"] = filtered
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(
        json.dumps(filtered_profile, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
