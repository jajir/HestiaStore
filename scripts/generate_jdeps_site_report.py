#!/usr/bin/env python3
"""Generate an HTML site page from jdeps DOT output."""

from __future__ import annotations

import argparse
import html
import re
import shutil
import subprocess
from pathlib import Path

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


def html_text(value: object) -> str:
    return html.escape(str(value))


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


def render_png(summary_dot: Path, png_path: Path) -> bool:
    dot_executable = shutil.which("dot")
    if not dot_executable:
        return False

    png_path.parent.mkdir(parents=True, exist_ok=True)
    subprocess.run(
        [dot_executable, "-Tpng", str(summary_dot), "-o", str(png_path)],
        check=True,
    )
    return True


def summary_cards(cards: list[tuple[str, object]]) -> str:
    items = "\n".join(
        (
            f'<div class="card"><span class="metric">{html_text(value)}</span>'
            f'<span class="metric-label">{html_text(label)}</span></div>'
        )
        for label, value in cards
    )
    return f'<section class="cards">{items}</section>'


def table_section(title: str, headers: list[str], rows: list[list[object]]) -> str:
    header_html = "".join(f"<th>{html_text(header)}</th>" for header in headers)
    body_rows = []
    for row in rows:
        columns = "".join(f"<td>{html_text(column)}</td>" for column in row)
        body_rows.append(f"<tr>{columns}</tr>")
    rows_html = "\n".join(body_rows) if body_rows else "<tr><td colspan=\"99\">No rows.</td></tr>"
    return (
        f'<section class="panel"><h2>{html_text(title)}</h2><table>'
        f"<thead><tr>{header_html}</tr></thead><tbody>{rows_html}</tbody></table></section>"
    )


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


def html_page(title: str, intro: str, image_html: str, sections: list[str]) -> str:
    sections_html = "\n".join(sections)
    return f"""<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1.0" />
  <title>{html_text(title)}</title>
  <style>
    :root {{
      color-scheme: light;
      --bg: #f8fafc;
      --panel: #ffffff;
      --border: #dbe3ee;
      --text: #0f172a;
      --muted: #475569;
      --accent: #1d4ed8;
    }}
    body {{
      margin: 0;
      padding: 2rem;
      background: linear-gradient(180deg, #eef4ff 0%, var(--bg) 220px);
      color: var(--text);
      font: 16px/1.5 "IBM Plex Sans", "Segoe UI", sans-serif;
    }}
    main {{
      max-width: 1200px;
      margin: 0 auto;
    }}
    .lead {{
      max-width: 72ch;
      color: var(--muted);
    }}
    .panel {{
      margin-top: 1.5rem;
      padding: 1.25rem;
      border: 1px solid var(--border);
      border-radius: 18px;
      background: var(--panel);
      box-shadow: 0 16px 48px rgba(15, 23, 42, 0.06);
    }}
    .cards {{
      display: grid;
      grid-template-columns: repeat(auto-fit, minmax(180px, 1fr));
      gap: 1rem;
      margin-top: 1.25rem;
    }}
    .card {{
      padding: 1rem;
      border-radius: 14px;
      background: #f8fbff;
      border: 1px solid var(--border);
    }}
    .metric {{
      display: block;
      font-size: 1.8rem;
      font-weight: 700;
    }}
    .metric-label, .note {{
      color: var(--muted);
    }}
    table {{
      width: 100%;
      border-collapse: collapse;
    }}
    th, td {{
      padding: 0.7rem;
      border-bottom: 1px solid var(--border);
      text-align: left;
      vertical-align: top;
    }}
    th {{
      font-size: 0.92rem;
      color: var(--muted);
    }}
    img {{
      display: block;
      width: 100%;
      height: auto;
      border-radius: 14px;
      border: 1px solid var(--border);
      background: #fff;
    }}
    a {{
      color: var(--accent);
    }}
    code {{
      background: #eff6ff;
      border-radius: 6px;
      padding: 0.1rem 0.35rem;
    }}
  </style>
</head>
<body>
  <main>
    <h1>{html_text(title)}</h1>
    <p class="lead">{intro}</p>
    {image_html}
    {sections_html}
  </main>
</body>
</html>
"""


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

    generated = render_png(summary_dot, png_path)

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

    html_path.parent.mkdir(parents=True, exist_ok=True)
    html_path.write_text(html_content, encoding="utf-8")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
