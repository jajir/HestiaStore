#!/usr/bin/env python3
"""Sync generated benchmark docs into canonical MkDocs filenames."""

from __future__ import annotations

import argparse
import shutil
import sys
from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class BenchmarkDocMapping:
    canonical_markdown: str
    canonical_chart_svg: str
    canonical_percentiles_svg: str
    candidate_markdown: tuple[str, ...]
    candidate_chart_svg: tuple[str, ...]
    candidate_percentiles_svg: tuple[str, ...]


MAPPINGS = (
    BenchmarkDocMapping(
        canonical_markdown="out-write.md",
        canonical_chart_svg="out-write.svg",
        canonical_percentiles_svg="out-write-percentiles.svg",
        candidate_markdown=("out-write-single-thread.md", "out-write.md"),
        candidate_chart_svg=("out-write-single-thread.svg", "out-write.svg"),
        candidate_percentiles_svg=(
            "out-write-single-thread-percentiles.svg",
            "out-write-percentiles.svg",
        ),
    ),
    BenchmarkDocMapping(
        canonical_markdown="out-read.md",
        canonical_chart_svg="out-read.svg",
        canonical_percentiles_svg="out-read-percentiles.svg",
        candidate_markdown=("out-read-single-thread.md", "out-read.md"),
        candidate_chart_svg=("out-read-single-thread.svg", "out-read.svg"),
        candidate_percentiles_svg=(
            "out-read-single-thread-percentiles.svg",
            "out-read-percentiles.svg",
        ),
    ),
    BenchmarkDocMapping(
        canonical_markdown="out-sequential.md",
        canonical_chart_svg="out-sequential.svg",
        canonical_percentiles_svg="out-sequential-percentiles.svg",
        candidate_markdown=("out-sequential-read.md", "out-sequential.md"),
        candidate_chart_svg=("out-sequential-read.svg", "out-sequential.svg"),
        candidate_percentiles_svg=(
            "out-sequential-read-percentiles.svg",
            "out-sequential-percentiles.svg",
        ),
    ),
    BenchmarkDocMapping(
        canonical_markdown="out-multithread-write.md",
        canonical_chart_svg="out-multithread-write.svg",
        canonical_percentiles_svg="out-multithread-write-percentiles.svg",
        candidate_markdown=("out-write-multi-thread.md", "out-multithread-write.md"),
        candidate_chart_svg=("out-write-multi-thread.svg", "out-multithread-write.svg"),
        candidate_percentiles_svg=(
            "out-write-multi-thread-percentiles.svg",
            "out-multithread-write-percentiles.svg",
        ),
    ),
    BenchmarkDocMapping(
        canonical_markdown="out-multithread-read.md",
        canonical_chart_svg="out-multithread-read.svg",
        canonical_percentiles_svg="out-multithread-read-percentiles.svg",
        candidate_markdown=("out-read-multi-thread.md", "out-multithread-read.md"),
        candidate_chart_svg=("out-read-multi-thread.svg", "out-multithread-read.svg"),
        candidate_percentiles_svg=(
            "out-read-multi-thread-percentiles.svg",
            "out-multithread-read-percentiles.svg",
        ),
    ),
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--source-root",
        required=True,
        help="Repository or staging root containing docs/why-hestiastore and docs/images.",
    )
    parser.add_argument(
        "--target-root",
        required=True,
        help="Repository root to receive canonical MkDocs benchmark docs.",
    )
    return parser.parse_args()


def benchmark_docs_dir(root: Path) -> Path:
    return root / "docs" / "why-hestiastore"


def benchmark_images_dir(root: Path) -> Path:
    return root / "docs" / "images"


def choose_latest(base_dir: Path, candidates: tuple[str, ...]) -> Path:
    existing = [base_dir / candidate for candidate in candidates if (base_dir / candidate).is_file()]
    if not existing:
        raise FileNotFoundError(
            f"Missing required benchmark artifact in {base_dir}: one of {', '.join(candidates)}"
        )
    return max(existing, key=lambda path: path.stat().st_mtime_ns)


def rewrite_markdown_images(markdown: str, mapping: BenchmarkDocMapping) -> str:
    rewritten = markdown
    for candidate in mapping.candidate_chart_svg:
        rewritten = rewritten.replace(
            f"../images/{candidate}",
            f"../images/{mapping.canonical_chart_svg}",
        )
    for candidate in mapping.candidate_percentiles_svg:
        rewritten = rewritten.replace(
            f"../images/{candidate}",
            f"../images/{mapping.canonical_percentiles_svg}",
        )
    return rewritten


def copy_markdown(source: Path, target: Path, mapping: BenchmarkDocMapping) -> None:
    text = rewrite_markdown_images(source.read_text(encoding="utf-8"), mapping)
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(text, encoding="utf-8")


def copy_binary(source: Path, target: Path) -> None:
    target.parent.mkdir(parents=True, exist_ok=True)
    if source.resolve() == target.resolve():
        return
    shutil.copy2(source, target)


def cleanup_obsolete_files(target_root: Path) -> None:
    target_docs = benchmark_docs_dir(target_root)
    target_images = benchmark_images_dir(target_root)
    canonical_markdown = {mapping.canonical_markdown for mapping in MAPPINGS}
    canonical_images = {
        mapping.canonical_chart_svg for mapping in MAPPINGS
    } | {
        mapping.canonical_percentiles_svg for mapping in MAPPINGS
    }

    obsolete_files: list[Path] = []
    for mapping in MAPPINGS:
        obsolete_files.extend(
            target_docs / name
            for name in mapping.candidate_markdown
            if name not in canonical_markdown
        )
        obsolete_files.extend(
            target_images / name
            for name in mapping.candidate_chart_svg
            if name not in canonical_images
        )
        obsolete_files.extend(
            target_images / name
            for name in mapping.candidate_percentiles_svg
            if name not in canonical_images
        )

    for path in obsolete_files:
        if path.exists():
            path.unlink()


def sync_mapping(source_root: Path, target_root: Path, mapping: BenchmarkDocMapping) -> None:
    source_docs = benchmark_docs_dir(source_root)
    source_images = benchmark_images_dir(source_root)
    target_docs = benchmark_docs_dir(target_root)
    target_images = benchmark_images_dir(target_root)

    source_markdown = choose_latest(source_docs, mapping.candidate_markdown)
    source_chart = choose_latest(source_images, mapping.candidate_chart_svg)
    source_percentiles = choose_latest(source_images, mapping.candidate_percentiles_svg)

    copy_markdown(source_markdown, target_docs / mapping.canonical_markdown, mapping)
    copy_binary(source_chart, target_images / mapping.canonical_chart_svg)
    copy_binary(
        source_percentiles,
        target_images / mapping.canonical_percentiles_svg,
    )


def main() -> int:
    args = parse_args()
    source_root = Path(args.source_root).resolve()
    target_root = Path(args.target_root).resolve()

    for mapping in MAPPINGS:
        sync_mapping(source_root, target_root, mapping)

    cleanup_obsolete_files(target_root)
    return 0


if __name__ == "__main__":
    sys.exit(main())
