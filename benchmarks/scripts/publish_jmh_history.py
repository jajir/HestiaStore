#!/usr/bin/env python3
"""Publish one benchmark profile run into a long-lived history tree."""

from __future__ import annotations

import argparse
import json
import shutil
import sys
from pathlib import Path
from typing import Any


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--source-dir", required=True,
                        help="Directory produced by run_jmh_profile.py.")
    parser.add_argument("--history-dir", required=True,
                        help="Root of the checked-out perf-artifacts branch.")
    parser.add_argument("--channel", default="main",
                        help="History channel name used for latest pointers.")
    parser.add_argument("--pr-number", default="",
                        help="Optional pull request number for PR-scoped history publishing.")
    parser.add_argument("--run-suffix", default="",
                        help="Optional suffix to make repeated runs unique.")
    parser.add_argument("--comparison-markdown", default="")
    parser.add_argument("--comparison-json", default="")
    parser.add_argument("--metadata-out", default="",
                        help="Optional path for machine-readable publish metadata.")
    return parser.parse_args()


def load_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def sanitize_timestamp(timestamp_utc: str) -> str:
    sanitized = timestamp_utc.replace(":", "-")
    sanitized = sanitized.replace("+00:00", "Z")
    return sanitized


def copy_if_present(source: Path, target: Path) -> str | None:
    if not source or not source.is_file():
        return None
    target.parent.mkdir(parents=True, exist_ok=True)
    shutil.copy2(source, target)
    return str(target)


def profile_root(profile: str, pr_number: str) -> Path:
    root = Path("history") / profile
    if pr_number:
        return root / "pull-requests" / f"pr-{pr_number}"
    return root


def main() -> int:
    args = parse_args()
    source_dir = Path(args.source_dir).resolve()
    history_dir = Path(args.history_dir).resolve()
    source_summary_path = source_dir / "summary.json"
    if not source_summary_path.is_file():
        raise FileNotFoundError(f"Missing summary.json in {source_dir}")
    summary = load_json(source_summary_path)

    profile = summary["profile"]
    timestamp_utc = summary["timestampUtc"]
    sha = summary["git"]["sha"]
    short_sha = sha[:12] if sha else "unknown"
    safe_timestamp = sanitize_timestamp(timestamp_utc)
    run_name = f"{safe_timestamp}_{short_sha}"
    if args.run_suffix:
        run_name = f"{run_name}_{args.run_suffix}"

    pr_number = str(args.pr_number).strip()
    scoped_profile_root = profile_root(profile, pr_number)
    run_dir_relative = scoped_profile_root / safe_timestamp[0:4] / safe_timestamp[5:7] / run_name
    run_dir = history_dir / run_dir_relative
    run_dir.parent.mkdir(parents=True, exist_ok=True)
    if run_dir.exists():
        shutil.rmtree(run_dir)
    shutil.copytree(source_dir, run_dir)

    comparison_markdown_relative = None
    comparison_json_relative = None
    if args.comparison_markdown:
        copied = copy_if_present(
            Path(args.comparison_markdown).resolve(),
            run_dir / "comparison-vs-previous.md",
        )
        if copied is not None:
            comparison_markdown_relative = str((run_dir_relative / "comparison-vs-previous.md").as_posix())
    if args.comparison_json:
        copied = copy_if_present(
            Path(args.comparison_json).resolve(),
            run_dir / "comparison-vs-previous.json",
        )
        if copied is not None:
            comparison_json_relative = str((run_dir_relative / "comparison-vs-previous.json").as_posix())

    if pr_number:
        latest_pointer_relative = scoped_profile_root / "latest.json"
    else:
        latest_pointer_relative = scoped_profile_root / f"latest-{args.channel}.json"
    latest_pointer_path = history_dir / latest_pointer_relative
    latest_pointer_path.parent.mkdir(parents=True, exist_ok=True)

    pointer = {
        "profile": profile,
        "channel": args.channel,
        "prNumber": pr_number or None,
        "timestampUtc": timestamp_utc,
        "sha": sha,
        "branch": summary["git"].get("branch", ""),
        "summaryPath": str((run_dir_relative / "summary.json").as_posix()),
        "runPath": str(run_dir_relative.as_posix()),
        "comparisonMarkdownPath": comparison_markdown_relative,
        "comparisonJsonPath": comparison_json_relative,
    }
    latest_pointer_path.write_text(
        json.dumps(pointer, indent=2, sort_keys=True),
        encoding="utf-8",
    )

    index_path = history_dir / "history" / "index.json"
    if index_path.is_file():
        index = load_json(index_path)
    else:
        index = {"profiles": {}}
    index.setdefault("profiles", {})
    profile_index = index["profiles"].setdefault(profile, {})
    channel_entry = {
        "latestPointerPath": str(latest_pointer_relative.as_posix()),
        "timestampUtc": timestamp_utc,
        "sha": sha,
    }
    if pr_number:
        profile_index.setdefault("pullRequests", {})
        profile_index["pullRequests"][pr_number] = {
            "channel": args.channel,
            **channel_entry,
        }
    else:
        profile_index.setdefault("channels", {})
        profile_index["channels"][args.channel] = dict(channel_entry)
        profile_index["latestChannel"] = args.channel
        profile_index["latestPointerPath"] = str(latest_pointer_relative.as_posix())
        profile_index["timestampUtc"] = timestamp_utc
        profile_index["sha"] = sha
    index_path.parent.mkdir(parents=True, exist_ok=True)
    index_path.write_text(json.dumps(index, indent=2, sort_keys=True), encoding="utf-8")

    if args.metadata_out:
        metadata = {
            "profile": profile,
            "channel": args.channel,
            "prNumber": pr_number or None,
            "timestampUtc": timestamp_utc,
            "sha": sha,
            "latestPointerPath": str(latest_pointer_relative.as_posix()),
            "runPath": str(run_dir_relative.as_posix()),
            "summaryPath": str((run_dir_relative / "summary.json").as_posix()),
            "comparisonMarkdownPath": comparison_markdown_relative,
            "comparisonJsonPath": comparison_json_relative,
        }
        metadata_path = Path(args.metadata_out).resolve()
        metadata_path.parent.mkdir(parents=True, exist_ok=True)
        metadata_path.write_text(
            json.dumps(metadata, indent=2, sort_keys=True),
            encoding="utf-8",
        )
    return 0


if __name__ == "__main__":
    sys.exit(main())
