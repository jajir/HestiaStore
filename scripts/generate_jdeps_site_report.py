#!/usr/bin/env python3
"""Generate an HTML site page from jdeps DOT output."""

from __future__ import annotations

import argparse
import re
from pathlib import Path

from site_report import html_page
from site_report import html_text
from site_report import render_dot_png
from site_report import summary_cards
from site_report import table_section
from site_report import write_text

EDGE_PATTERN = re.compile(r'^\s*"(?P<source>.+?)"\s*->\s*"(?P<target>.+?)";\s*$')


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--summary-dot", required=True)
    parser.add_argument("--raw-dir", required=True)
    parser.add_argument("--html", dest="html_path", required=True)
    parser.add_argument("--png", dest="png_path", required=True)
    parser.add_argument("--page-title", required=True)
    parser.add_argument("--multi-release", required=True)
    return parser.parse_args()


def parse_edges(summary_dot: Path) -> list[tuple[str, str]]:
    edges: list[tuple[str, str]] = []
    for line in summary_dot.read_text(encoding="utf-8").splitlines():
        match = EDGE_PATTERN.match(line)
        if match:
            edges.append((match.group("source"), match.group("target")))
    return edges


def classify_target(target: str) -> str:
    if target == "not found":
        return "Unresolved"
    if target.startswith("java.") or target.startswith("jdk."):
        return "JDK module"
    return "Classpath dependency"


def image_section(image_rel_path: str, generated: bool) -> str:
    if generated:
        body = (
            f'<img src="{html_text(image_rel_path)}" alt="JDeps dependency graph" />'
            '<p class="note">Rendered directly from the DOT files emitted by <code>jdeps</code>.</p>'
        )
    else:
        body = (
            '<p class="note">Graphviz <code>dot</code> was not available, '
            "so the page includes only tables and raw DOT links.</p>"
        )
    return f'<section class="panel"><h2>JDeps Summary Graph</h2>{body}</section>'


def main() -> int:
    args = parse_args()
    summary_dot = Path(args.summary_dot)
    raw_dir = Path(args.raw_dir)
    html_path = Path(args.html_path)
    png_path = Path(args.png_path)

    edges = parse_edges(summary_dot)
    nodes = sorted({value for edge in edges for value in edge})
    raw_files = sorted(raw_dir.glob("*.dot"))

    jdk_targets = sum(1 for _, target in edges if classify_target(target) == "JDK module")
    unresolved_targets = sum(1 for _, target in edges if classify_target(target) == "Unresolved")
    classpath_targets = sum(1 for _, target in edges if classify_target(target) == "Classpath dependency")

    generated = render_dot_png(summary_dot, png_path)

    edge_rows = [[source, target, classify_target(target)] for source, target in edges]
    raw_rows = [[path.name, f"jdeps/{path.name}"] for path in raw_files]

    cards = summary_cards(
        [
            ("Nodes in summary graph", len(nodes)),
            ("Dependency edges", len(edges)),
            ("JDK module targets", jdk_targets),
            ("Classpath targets", classpath_targets),
            ("Unresolved targets", unresolved_targets),
        ]
    )

    note = (
        '<section class="panel"><h2>What This Page Shows</h2>'
        "<p class=\"note\">This page is built from <code>maven-jdeps-plugin:jdkinternals</code> "
        "with DOT output enabled and <code>--multi-release "
        f"{html_text(args.multi_release)}</code>. The goal still prints any internal-JDK findings "
        "to the Maven log during the build; this page focuses on the generated graph artifacts.</p>"
        "</section>"
    )

    sections = [
        cards,
        note,
        table_section("Summary Edges", ["From", "To", "Kind"], edge_rows),
        table_section("Raw DOT Files", ["File", "Relative Path"], raw_rows),
    ]

    intro = (
        "JDeps analyzes compiled bytecode. In this setup it runs against the engine main classes and "
        "its compile-scope dependencies, then exposes the generated DOT files through the site."
    )
    html_content = html_page(
        args.page_title,
        intro,
        image_section("jdeps/jdeps-summary.png", generated),
        sections,
    )

    write_text(html_path, html_content)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
