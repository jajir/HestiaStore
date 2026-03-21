#!/usr/bin/env python3
"""Compare two JMH profile runs and emit markdown + machine-readable summary."""

from __future__ import annotations

import argparse
import json
import math
import sys
from pathlib import Path
from typing import Any

DIAGNOSTIC_SECONDARY_PREFIX = "diag_"


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
                "scoreError": row["primaryMetric"].get("scoreError"),
                "unit": row["primaryMetric"]["scoreUnit"],
            }
            for secondary_name, secondary_metric in row.get("secondaryMetrics", {}).items():
                if secondary_name.startswith(DIAGNOSTIC_SECONDARY_PREFIX):
                    continue
                metrics[f"{label}::{method}::secondary::{secondary_name}"] = {
                    "label": label,
                    "method": method,
                    "metric": secondary_name,
                    "benchmark": row["benchmark"],
                    "score": secondary_metric["score"],
                    "scoreError": secondary_metric.get("scoreError"),
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


def relative_error_pct(score: float | None, score_error: float | None) -> float | None:
    numeric_score = optional_float(score)
    numeric_score_error = optional_float(score_error)
    if numeric_score is None or numeric_score_error is None:
        return None
    if math.isclose(numeric_score, 0.0, abs_tol=1e-12):
        return None
    return abs(numeric_score_error / numeric_score) * 100.0


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
        baseline_display = (
            "-"
            if row["baselineScore"] is None
            else f"{row['baselineScore']:.3f} {row['unit']}"
        )
        candidate_display = (
            "-"
            if row["candidateScore"] is None
            else f"{row['candidateScore']:.3f} {row['unit']}"
        )
        delta_display = (
            "-"
            if row["deltaPct"] is None
            else f"{row['deltaPct']:+.2f}%"
        )
        lines.append(
            f"| `{row['displayName']}` | `{baseline_display}` | "
            f"`{candidate_display}` | `{delta_display}` | `{row['status']}` |"
        )
    return "\n".join(lines) + "\n"


def main() -> int:
    args = parse_args()
    baseline_summary = load_summary(args.baseline)
    candidate_summary = load_summary(args.candidate)
    baseline_metrics = flatten(baseline_summary)
    candidate_metrics = flatten(candidate_summary)

    shared_keys = sorted(set(baseline_metrics) & set(candidate_metrics))
    candidate_only_keys = sorted(set(candidate_metrics) - set(baseline_metrics))
    baseline_only_keys = sorted(set(baseline_metrics) - set(candidate_metrics))
    comparison_rows: list[dict[str, Any]] = []
    worse_count = 0
    new_metric_count = 0
    removed_metric_count = 0

    for key in shared_keys:
        baseline_metric = baseline_metrics[key]
        candidate_metric = candidate_metrics[key]
        if baseline_metric["unit"] != candidate_metric["unit"]:
            raise ValueError(f"Metric unit mismatch for {key}: {baseline_metric['unit']} vs {candidate_metric['unit']}")
        baseline_score = baseline_metric["score"]
        candidate_score = candidate_metric["score"]
        baseline_score_error = baseline_metric.get("scoreError")
        candidate_score_error = candidate_metric.get("scoreError")
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
            "label": candidate_metric["label"],
            "method": candidate_metric["method"],
            "metric": candidate_metric["metric"],
            "baselineScore": baseline_score,
            "baselineScoreError": baseline_score_error,
            "baselineScoreErrorPct": relative_error_pct(baseline_score, baseline_score_error),
            "candidateScore": candidate_score,
            "candidateScoreError": candidate_score_error,
            "candidateScoreErrorPct": relative_error_pct(candidate_score, candidate_score_error),
            "deltaPct": delta_pct,
            "status": status,
            "unit": candidate_metric["unit"],
        })

    for key in candidate_only_keys:
        candidate_metric = candidate_metrics[key]
        new_metric_count += 1
        comparison_rows.append({
            "key": key,
            "displayName": display_metric_name(key, candidate_metric),
            "label": candidate_metric["label"],
            "method": candidate_metric["method"],
            "metric": candidate_metric["metric"],
            "baselineScore": None,
            "baselineScoreError": None,
            "baselineScoreErrorPct": None,
            "candidateScore": candidate_metric["score"],
            "candidateScoreError": candidate_metric.get("scoreError"),
            "candidateScoreErrorPct": relative_error_pct(candidate_metric["score"], candidate_metric.get("scoreError")),
            "deltaPct": None,
            "status": "new",
            "unit": candidate_metric["unit"],
        })

    for key in baseline_only_keys:
        baseline_metric = baseline_metrics[key]
        removed_metric_count += 1
        comparison_rows.append({
            "key": key,
            "displayName": display_metric_name(key, baseline_metric),
            "label": baseline_metric["label"],
            "method": baseline_metric["method"],
            "metric": baseline_metric["metric"],
            "baselineScore": baseline_metric["score"],
            "baselineScoreError": baseline_metric.get("scoreError"),
            "baselineScoreErrorPct": relative_error_pct(baseline_metric["score"], baseline_metric.get("scoreError")),
            "candidateScore": None,
            "candidateScoreError": None,
            "candidateScoreErrorPct": None,
            "deltaPct": None,
            "status": "removed",
            "unit": baseline_metric["unit"],
        })

    comparison_rows.sort(key=lambda row: row["displayName"])

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
        "newMetricCount": new_metric_count,
        "removedMetricCount": removed_metric_count,
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
