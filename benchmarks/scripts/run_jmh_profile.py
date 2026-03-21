#!/usr/bin/env python3
"""Run a canonical JMH benchmark profile and persist raw results + metadata."""

from __future__ import annotations

import argparse
import json
import math
import os
import platform
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--repo-root", required=True)
    parser.add_argument("--profile", required=True,
                        help="Profile name under benchmarks/profiles or path to a profile json file.")
    parser.add_argument("--output-dir", required=True)
    parser.add_argument("--jar", default="")
    parser.add_argument("--skip-build", action="store_true")
    parser.add_argument("--maven-cmd", default="mvn")
    return parser.parse_args()


def resolve_profile_path(repo_root: Path, profile_arg: str) -> Path:
    candidate = Path(profile_arg)
    if candidate.is_file():
        return candidate.resolve()
    profile_path = repo_root / "benchmarks" / "profiles" / f"{profile_arg}.json"
    if profile_path.is_file():
        return profile_path
    raise FileNotFoundError(f"Unable to resolve benchmark profile '{profile_arg}'.")


def run_command(cmd: list[str], cwd: Path, stdout_path: Path | None = None) -> None:
    stdout_handle = None
    try:
        if stdout_path is not None:
            stdout_path.parent.mkdir(parents=True, exist_ok=True)
            stdout_handle = stdout_path.open("w", encoding="utf-8")
            process = subprocess.run(
                cmd,
                cwd=str(cwd),
                stdout=stdout_handle,
                stderr=subprocess.STDOUT,
                text=True,
                check=False,
            )
        else:
            process = subprocess.run(cmd, cwd=str(cwd), text=True, check=False)
        if process.returncode != 0:
            raise subprocess.CalledProcessError(process.returncode, cmd)
    finally:
        if stdout_handle is not None:
            stdout_handle.close()


def capture_output(cmd: list[str], cwd: Path) -> str:
    process = subprocess.run(
        cmd,
        cwd=str(cwd),
        text=True,
        capture_output=True,
        check=False,
    )
    if process.returncode != 0:
        raise subprocess.CalledProcessError(process.returncode, cmd, process.stdout, process.stderr)
    return process.stdout.strip()


def resolve_jar(repo_root: Path, explicit_jar: str) -> Path:
    if explicit_jar:
        jar_path = Path(explicit_jar)
        if not jar_path.is_absolute():
            jar_path = (repo_root / jar_path).resolve()
        return jar_path
    jars = sorted((repo_root / "benchmarks" / "target").glob("benchmarks-*.jar"))
    jars = [jar for jar in jars if not jar.name.startswith("original-")]
    if not jars:
        raise FileNotFoundError("Unable to find packaged JMH runner jar under benchmarks/target.")
    return jars[0]


def build_if_needed(repo_root: Path, maven_cmd: str) -> None:
    run_command(
        [maven_cmd, "-q", "-pl", "benchmarks", "-am", "-DskipTests", "package"],
        cwd=repo_root,
    )


def git_metadata(repo_root: Path) -> dict[str, Any]:
    def maybe_capture(cmd: list[str]) -> str:
        try:
            return capture_output(cmd, repo_root)
        except subprocess.CalledProcessError:
            return ""

    status = maybe_capture(["git", "status", "--porcelain"])
    branch = maybe_capture(["git", "rev-parse", "--abbrev-ref", "HEAD"])
    return {
        "sha": maybe_capture(["git", "rev-parse", "HEAD"]),
        "branch": branch,
        "dirty": bool(status),
        "mergeBaseFriendlyRef": maybe_capture(["git", "describe", "--always", "--dirty", "--tags"]),
    }


def host_metadata() -> dict[str, Any]:
    cpu = platform.processor()
    if not cpu:
        cpu = platform.machine()
    return {
        "hostname": platform.node(),
        "os": platform.platform(),
        "machine": platform.machine(),
        "processor": cpu,
        "python": platform.python_version(),
    }


def normalize_result(result_path: Path) -> dict[str, Any]:
    rows = json.loads(result_path.read_text(encoding="utf-8"))
    normalized_rows = []
    for row in rows:
        normalized_rows.append({
            "benchmark": row["benchmark"],
            "mode": row["mode"],
            "threads": row["threads"],
            "params": row.get("params", {}),
            "primaryMetric": {
                "score": row["primaryMetric"]["score"],
                "scoreError": normalize_optional_number(
                    row["primaryMetric"].get("scoreError")),
                "scoreUnit": row["primaryMetric"]["scoreUnit"],
            },
            "secondaryMetrics": {
                name: {
                    "score": metric["score"],
                    "scoreError": normalize_optional_number(
                        metric.get("scoreError")),
                    "scoreUnit": metric["scoreUnit"],
                }
                for name, metric in row.get("secondaryMetrics", {}).items()
            },
            "jdkVersion": row.get("jdkVersion"),
            "vmVersion": row.get("vmVersion"),
        })
    return {"results": normalized_rows}


def normalize_optional_number(value: Any) -> float | None:
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


def main() -> int:
    args = parse_args()
    repo_root = Path(args.repo_root).resolve()
    profile_path = resolve_profile_path(repo_root, args.profile)
    output_dir = Path(args.output_dir).resolve()
    output_dir.mkdir(parents=True, exist_ok=True)
    profile = json.loads(profile_path.read_text(encoding="utf-8"))

    if not args.skip_build:
        build_if_needed(repo_root, args.maven_cmd)
    jar_path = resolve_jar(repo_root, args.jar)

    raw_dir = output_dir / "raw"
    logs_dir = output_dir / "logs"
    raw_dir.mkdir(parents=True, exist_ok=True)
    logs_dir.mkdir(parents=True, exist_ok=True)

    run_summary: dict[str, Any] = {
        "profile": profile["profile"],
        "description": profile.get("description", ""),
        "timestampUtc": datetime.now(timezone.utc).isoformat(),
        "repoRoot": str(repo_root),
        "jar": str(jar_path),
        "git": git_metadata(repo_root),
        "host": host_metadata(),
        "benchmarks": [],
    }

    for benchmark in profile["benchmarks"]:
        label = benchmark["label"]
        raw_path = raw_dir / f"{label}.json"
        log_path = logs_dir / f"{label}.log"
        cmd = ["java", "-jar", str(jar_path), benchmark["include"], *benchmark["args"], "-rf", "json", "-rff", str(raw_path)]
        run_command(cmd, cwd=repo_root, stdout_path=log_path)
        normalized = normalize_result(raw_path)
        run_summary["benchmarks"].append({
            "label": label,
            "include": benchmark["include"],
            "args": benchmark["args"],
            "rawResult": os.path.relpath(raw_path, output_dir),
            "log": os.path.relpath(log_path, output_dir),
            "normalized": normalized,
        })

    (output_dir / "summary.json").write_text(
        json.dumps(run_summary, indent=2, sort_keys=True),
        encoding="utf-8",
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
