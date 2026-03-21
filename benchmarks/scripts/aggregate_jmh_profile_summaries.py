#!/usr/bin/env python3
"""Median-merge selected benchmark labels across repeated profile summaries."""

from __future__ import annotations

import argparse
import copy
import json
import math
import statistics
import sys
from pathlib import Path
from typing import Any


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--base-summary", required=True)
    parser.add_argument("--supplemental-summary", action="append", default=[])
    parser.add_argument("--labels", required=True,
                        help="Comma-separated benchmark labels to median-merge.")
    parser.add_argument("--output", required=True)
    return parser.parse_args()


def load_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def median_or_none(values: list[float | None]) -> float | None:
    present = [
        numeric
        for value in values
        for numeric in [optional_float(value)]
        if numeric is not None
    ]
    if not present:
        return None
    return float(statistics.median(present))


def collect_row_by_benchmark(
        benchmark: dict[str, Any]) -> dict[str, dict[str, Any]]:
    results = benchmark.get("normalized", {}).get("results", [])
    return {
        str(row.get("benchmark")): row
        for row in results
    }


def merge_metric(
        base_metric: dict[str, Any],
        candidate_metrics: list[dict[str, Any]]) -> dict[str, Any]:
    merged = dict(base_metric)
    merged["score"] = statistics.median(
        [float(metric["score"]) for metric in candidate_metrics]
    )
    merged["scoreError"] = median_or_none([
        metric.get("scoreError")
        for metric in candidate_metrics
    ])
    return merged


def optional_float(value: Any) -> float | None:
    if value is None:
        return None
    if isinstance(value, (int, float)):
        numeric = float(value)
    elif isinstance(value, str):
        try:
            numeric = float(value)
        except ValueError:
            return None
    else:
        return None
    if not math.isfinite(numeric):
        return None
    return numeric


def merge_result_row(
        base_row: dict[str, Any],
        candidate_rows: list[dict[str, Any]]) -> dict[str, Any]:
    merged_row = copy.deepcopy(base_row)
    merged_row["primaryMetric"] = merge_metric(
        base_row["primaryMetric"],
        [row["primaryMetric"] for row in candidate_rows],
    )

    secondary_names: list[str] = list(base_row.get("secondaryMetrics", {}))
    for row in candidate_rows:
        for secondary_name in row.get("secondaryMetrics", {}):
            if secondary_name not in secondary_names:
                secondary_names.append(secondary_name)

    merged_secondaries: dict[str, Any] = {}
    for secondary_name in secondary_names:
        base_secondary = base_row.get("secondaryMetrics", {}).get(secondary_name)
        if base_secondary is None:
            for row in candidate_rows:
                base_secondary = row.get("secondaryMetrics", {}).get(
                    secondary_name)
                if base_secondary is not None:
                    break
        secondary_candidates = [
            row["secondaryMetrics"][secondary_name]
            for row in candidate_rows
            if secondary_name in row.get("secondaryMetrics", {})
        ]
        if base_secondary is None or not secondary_candidates:
            continue
        merged_secondaries[secondary_name] = merge_metric(
            base_secondary, secondary_candidates)
    merged_row["secondaryMetrics"] = merged_secondaries
    return merged_row


def merge_benchmark(
        base_benchmark: dict[str, Any],
        candidate_benchmarks: list[dict[str, Any]]) -> dict[str, Any]:
    merged_benchmark = copy.deepcopy(base_benchmark)
    base_rows = collect_row_by_benchmark(base_benchmark)
    results: list[dict[str, Any]] = []
    for benchmark_name, base_row in base_rows.items():
        candidate_rows = [
            row_by_name[benchmark_name]
            for row_by_name in (
                collect_row_by_benchmark(benchmark)
                for benchmark in candidate_benchmarks
            )
            if benchmark_name in row_by_name
        ]
        if not candidate_rows:
            results.append(copy.deepcopy(base_row))
            continue
        results.append(merge_result_row(base_row, candidate_rows))
    merged_benchmark["normalized"]["results"] = results
    return merged_benchmark


def main() -> int:
    args = parse_args()
    selected_labels = [
        label.strip()
        for label in args.labels.split(",")
        if label.strip()
    ]
    if not selected_labels:
        raise ValueError("At least one benchmark label is required.")

    base_summary_path = Path(args.base_summary).resolve()
    output_path = Path(args.output).resolve()
    base_summary = load_json(base_summary_path)
    supplemental_summaries = [
        load_json(Path(path).resolve())
        for path in args.supplemental_summary
    ]
    merged_summary = copy.deepcopy(base_summary)
    merged_summary.setdefault("aggregation", {})
    merged_summary["aggregation"]["medianMergedLabels"] = selected_labels
    merged_summary["aggregation"]["candidateRunCount"] = (
        1 + len(supplemental_summaries)
    )

    by_label: dict[str, dict[str, Any]] = {
        str(benchmark.get("label")): benchmark
        for benchmark in merged_summary.get("benchmarks", [])
    }
    for label in selected_labels:
        base_benchmark = by_label.get(label)
        if base_benchmark is None:
            raise ValueError(f"Missing benchmark label in base summary: {label}")
        candidate_benchmarks = []
        for summary in [base_summary, *supplemental_summaries]:
            benchmark = next(
                (entry for entry in summary.get("benchmarks", [])
                 if entry.get("label") == label),
                None,
            )
            if benchmark is not None:
                candidate_benchmarks.append(benchmark)
        by_label[label] = merge_benchmark(base_benchmark, candidate_benchmarks)

    merged_benchmarks = []
    for benchmark in merged_summary.get("benchmarks", []):
        label = str(benchmark.get("label"))
        merged_benchmarks.append(by_label.get(label, benchmark))
    merged_summary["benchmarks"] = merged_benchmarks

    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(
        json.dumps(merged_summary, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
