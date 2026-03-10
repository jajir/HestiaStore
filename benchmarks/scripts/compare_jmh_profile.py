#!/usr/bin/env python3
"""Compare two JMH profile runs and emit markdown + machine-readable summary."""

from __future__ import annotations

import argparse
import json
import math
import sys
from pathlib import Path
from typing import Any


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--baseline", required=True, help="Path to summary.json of the baseline run.")
    parser.add_argument("--candidate", required=True, help="Path to summary.json of the candidate run.")
    parser.add_argument("--markdown-out", required=True)
    parser.add_argument("--json-out", required=True)
    parser.add_argument("--neutral-threshold", type=float, default=3.0)
    parser.add_argument("--fail-threshold", type=float, default=7.0)
    parser.add_argument("--fail-on-regression", action="store_true")
    return parser.parse_args()


def load_summary(path_str: str) -> dict[str, Any]:
    path = Path(path_str)
    return json.loads(path.read_text(encoding="utf-8"))


def flatten(summary: dict[str, Any]) -> dict[str, dict[str, Any]]:
    metrics: dict[str, dict[str, Any]] = {}
    for benchmark in summary["benchmarks"]:
        label = benchmark["label"]
        for row in benchmark["normalized"]["results"]:
            method = row["benchmark"].split(".")[-1]
            metrics[f"{label}::{method}::primary"] = {
                "label": label,
                "method": method,
                "metric": "primary",
                "benchmark": row["benchmark"],
                "score": row["primaryMetric"]["score"],
                "unit": row["primaryMetric"]["scoreUnit"],
            }
            for secondary_name, secondary_metric in row.get("secondaryMetrics", {}).items():
                metrics[f"{label}::{method}::secondary::{secondary_name}"] = {
                    "label": label,
                    "method": method,
                    "metric": secondary_name,
                    "benchmark": row["benchmark"],
                    "score": secondary_metric["score"],
                    "unit": secondary_metric["scoreUnit"],
                }
    return metrics


def classify(delta_pct: float, neutral: float, fail: float) -> str:
    if math.isclose(delta_pct, 0.0, abs_tol=1e-9):
        return "neutral"
    if delta_pct > 0:
        return "better" if abs(delta_pct) >= neutral else "neutral"
    magnitude = abs(delta_pct)
    if magnitude >= fail:
        return "worse"
    if magnitude >= neutral:
        return "warning"
    return "neutral"


def display_metric_name(metric_key: str, metric: dict[str, Any]) -> str:
    parts = metric_key.split("::")
    if len(parts) == 3:
        return f"{parts[0]}:{parts[1]}"
    if len(parts) == 4:
        return f"{parts[0]}:{parts[1]}:{parts[-1]}"
    return metric_key


def markdown_report(
    baseline_summary: dict[str, Any],
    candidate_summary: dict[str, Any],
    comparison_rows: list[dict[str, Any]],
    neutral: float,
    fail: float,
) -> str:
    lines = [
        "# Benchmark Comparison",
        "",
        f"- Profile: `{candidate_summary['profile']}`",
        f"- Baseline SHA: `{baseline_summary['git']['sha']}`",
        f"- Candidate SHA: `{candidate_summary['git']['sha']}`",
        f"- Thresholds: neutral `<= {neutral:.1f}%`, fail `> {fail:.1f}%` regression",
        "",
        "| Metric | Baseline | Candidate | Delta | Status |",
        "| --- | ---: | ---: | ---: | --- |",
    ]
    for row in comparison_rows:
        lines.append(
            f"| `{row['displayName']}` | `{row['baselineScore']:.3f} {row['unit']}` | "
            f"`{row['candidateScore']:.3f} {row['unit']}` | `{row['deltaPct']:+.2f}%` | `{row['status']}` |"
        )
    return "\n".join(lines) + "\n"


def main() -> int:
    args = parse_args()
    baseline_summary = load_summary(args.baseline)
    candidate_summary = load_summary(args.candidate)
    baseline_metrics = flatten(baseline_summary)
    candidate_metrics = flatten(candidate_summary)

    shared_keys = sorted(set(baseline_metrics) & set(candidate_metrics))
    comparison_rows: list[dict[str, Any]] = []
    worse_count = 0

    for key in shared_keys:
        baseline_metric = baseline_metrics[key]
        candidate_metric = candidate_metrics[key]
        if baseline_metric["unit"] != candidate_metric["unit"]:
            raise ValueError(f"Metric unit mismatch for {key}: {baseline_metric['unit']} vs {candidate_metric['unit']}")
        baseline_score = baseline_metric["score"]
        candidate_score = candidate_metric["score"]
        if baseline_score == 0:
            delta_pct = 0.0 if candidate_score == 0 else 100.0
        else:
            delta_pct = ((candidate_score - baseline_score) / baseline_score) * 100.0
        status = classify(delta_pct, args.neutral_threshold, args.fail_threshold)
        if status == "worse":
            worse_count += 1
        comparison_rows.append({
            "key": key,
            "displayName": display_metric_name(key, candidate_metric),
            "baselineScore": baseline_score,
            "candidateScore": candidate_score,
            "deltaPct": delta_pct,
            "status": status,
            "unit": candidate_metric["unit"],
        })

    markdown = markdown_report(
        baseline_summary,
        candidate_summary,
        comparison_rows,
        args.neutral_threshold,
        args.fail_threshold,
    )
    Path(args.markdown_out).write_text(markdown, encoding="utf-8")

    summary = {
        "profile": candidate_summary["profile"],
        "baselineSha": baseline_summary["git"]["sha"],
        "candidateSha": candidate_summary["git"]["sha"],
        "neutralThresholdPct": args.neutral_threshold,
        "failThresholdPct": args.fail_threshold,
        "worseCount": worse_count,
        "metrics": comparison_rows,
    }
    Path(args.json_out).write_text(
        json.dumps(summary, indent=2, sort_keys=True),
        encoding="utf-8",
    )

    if args.fail_on_regression and worse_count > 0:
        return 2
    return 0


if __name__ == "__main__":
    sys.exit(main())
