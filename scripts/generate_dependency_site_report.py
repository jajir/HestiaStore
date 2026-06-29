#!/usr/bin/env python3
"""Generate custom Maven site pages from depgraph JSON output."""

from __future__ import annotations

import argparse
import html
import json
import shutil
import subprocess
import sys
from collections import defaultdict
from pathlib import Path

SCOPE_ORDER = {
    "compile": 0,
    "provided": 1,
    "runtime": 2,
    "system": 3,
    "test": 4,
}

SCOPE_COLORS = {
    "compile": "#dbeafe",
    "provided": "#fde68a",
    "runtime": "#bbf7d0",
    "system": "#f5d0fe",
    "test": "#e5e7eb",
}

DEGREE_COLORS = {
    0: "#f8fafc",
    1: "#e2e8f0",
    2: "#bfdbfe",
    3: "#93c5fd",
}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("mode", choices=("engine", "reactor"))
    parser.add_argument("--json", dest="json_path", required=True)
    parser.add_argument("--html", dest="html_path", required=True)
    parser.add_argument("--dot", dest="dot_path", required=True)
    parser.add_argument("--png", dest="png_path", required=True)
    parser.add_argument("--include-group-id", required=True)
    parser.add_argument("--page-title", required=True)
    parser.add_argument("--root-artifact")
    return parser.parse_args()


def sort_scopes(scopes: list[str]) -> list[str]:
    return sorted(scopes, key=lambda scope: SCOPE_ORDER.get(scope, len(SCOPE_ORDER)))


def scope_sort_key(artifact: dict[str, object]) -> tuple[int, str, str]:
    scopes = sort_scopes(list(artifact.get("scopes", [])))
    primary_scope = scopes[0] if scopes else "zz"
    return (
        SCOPE_ORDER.get(primary_scope, len(SCOPE_ORDER)),
        str(artifact["groupId"]),
        str(artifact["artifactId"]),
    )


def artifact_coords(artifact: dict[str, object]) -> str:
    return f"{artifact['groupId']}:{artifact['artifactId']}"


def artifact_label(artifact: dict[str, object]) -> str:
    version = str(artifact.get("version", ""))
    scopes = ", ".join(sort_scopes(list(artifact.get("scopes", []))))
    label_lines = [str(artifact["artifactId"])]
    if version:
        label_lines.append(version)
    if scopes:
        label_lines.append(scopes)
    return "\\n".join(label_lines)


def html_text(value: object) -> str:
    return html.escape(str(value))


def dot_text(value: object) -> str:
    return str(value).replace("\\", "\\\\").replace('"', '\\"')


def write_text(path: Path, content: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content, encoding="utf-8")


def render_png(dot_path: Path, png_path: Path) -> bool:
    dot_executable = shutil.which("dot")
    if not dot_executable:
        return False

    subprocess.run(
        [dot_executable, "-Tpng", str(dot_path), "-o", str(png_path)],
        check=True,
    )
    return True


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
    h1, h2 {{
      line-height: 1.2;
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
    .metric-label {{
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
    code {{
      background: #eff6ff;
      border-radius: 6px;
      padding: 0.1rem 0.35rem;
    }}
    .note {{
      color: var(--muted);
    }}
    a {{
      color: var(--accent);
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


def image_section(title: str, image_path: Path, generated: bool) -> str:
    if generated:
        image_html = (
            f'<img src="{html_text(image_path.as_posix())}" alt="{html_text(title)}" />'
            '<p class="note">Image generated from the same JSON source as the tables below.</p>'
        )
    else:
        image_html = (
            '<p class="note">Graphviz <code>dot</code> was not available, '
            "so only the table report was generated.</p>"
        )
    return f'<section class="panel"><h2>{html_text(title)}</h2>{image_html}</section>'


def degree_color(total_degree: int) -> str:
    if total_degree >= 3:
        return DEGREE_COLORS[3]
    return DEGREE_COLORS.get(total_degree, DEGREE_COLORS[0])


def reachable_nodes(start: str, adjacency: dict[str, set[str]]) -> set[str]:
    seen: set[str] = set()
    stack = list(adjacency.get(start, ()))
    while stack:
        current = stack.pop()
        if current in seen:
            continue
        seen.add(current)
        stack.extend(adjacency.get(current, ()))
    return seen


def build_engine_report(
    args: argparse.Namespace, data: dict[str, object]
) -> tuple[str, str, str, Path, list[str]]:
    root_group_id, root_artifact_id = args.root_artifact.split(":", 1)
    artifacts = {artifact["id"]: artifact for artifact in data["artifacts"]}

    root_artifact = next(
        (
            artifact
            for artifact in artifacts.values()
            if artifact["groupId"] == root_group_id and artifact["artifactId"] == root_artifact_id
        ),
        None,
    )
    if root_artifact is None:
        raise ValueError(f"Root artifact {args.root_artifact} was not found in {args.json_path}.")

    direct_external_edges = []
    for dependency in data["dependencies"]:
        if dependency["from"] != root_artifact["id"]:
            continue
        target_artifact = artifacts[dependency["to"]]
        if target_artifact["groupId"] == args.include_group_id:
            continue
        direct_external_edges.append((dependency, target_artifact))

    direct_external_edges.sort(key=lambda item: scope_sort_key(item[1]))
    direct_external_artifacts = [artifact for _, artifact in direct_external_edges]

    compile_like = 0
    test_only = 0
    for artifact in direct_external_artifacts:
        scopes = set(artifact.get("scopes", []))
        if scopes == {"test"}:
            test_only += 1
        else:
            compile_like += 1

    rows = []
    for _, artifact in direct_external_edges:
        rows.append(
            [
                artifact_coords(artifact),
                artifact.get("version", ""),
                ", ".join(sort_scopes(list(artifact.get("scopes", [])))),
                ", ".join(sorted(artifact.get("types", []))),
                "yes" if artifact.get("optional", False) else "no",
            ]
        )

    dot_lines = [
        'digraph "engine_external_dependencies" {',
        "  rankdir=LR;",
        '  graph [bgcolor="transparent", pad="0.2"];',
        '  node [shape=box, style="rounded,filled", color="#475569", fontname="Helvetica"];',
        '  edge [color="#64748b", arrowsize="0.8", fontname="Helvetica"];',
        (
            f'  "{dot_text(root_artifact["id"])}" '
            f'[label="{dot_text(root_artifact["artifactId"])}", fillcolor="#0f172a", fontcolor="white"];'
        ),
    ]

    for _, artifact in direct_external_edges:
        scopes = sort_scopes(list(artifact.get("scopes", [])))
        primary_scope = scopes[0] if scopes else "compile"
        dot_lines.append(
            (
                f'  "{dot_text(artifact["id"])}" '
                f'[label="{dot_text(artifact_label(artifact))}", '
                f'fillcolor="{SCOPE_COLORS.get(primary_scope, "#e2e8f0")}"];'
            )
        )
        dot_lines.append(
            (
                f'  "{dot_text(root_artifact["id"])}" -> "{dot_text(artifact["id"])}" '
                f'[label="{dot_text(", ".join(scopes))}"];'
            )
        )
    dot_lines.append("}")

    cards = summary_cards(
        [
            ("Direct external dependencies", len(direct_external_artifacts)),
            ("Production-oriented scopes", compile_like),
            ("Test-only scopes", test_only),
        ]
    )
    tables = [
        cards,
        (
            '<section class="panel"><h2>Standard Maven Tables</h2>'
            '<p class="note">The built-in Maven dependency table remains available in '
            '<a href="dependencies.html">Dependencies</a>. '
            "The custom page below keeps the direct external view compact.</p></section>"
        ),
        table_section(
            "Direct External Dependencies",
            ["Dependency", "Version", "Scopes", "Type", "Optional"],
            rows,
        ),
    ]

    intro = (
        "This page summarizes the direct external dependencies of the engine module. "
        "It keeps the graph focused on first-level dependencies and leaves the full transitive table "
        "to the standard Maven dependencies report."
    )
    return (
        "\n".join(dot_lines),
        intro,
        "Direct External Dependency Graph",
        Path("depgraph/engine-external-dependencies.png"),
        tables,
    )


def build_reactor_report(
    args: argparse.Namespace, data: dict[str, object]
) -> tuple[str, str, str, Path, list[str]]:
    artifacts = {
        artifact["id"]: artifact
        for artifact in data.get("artifacts", [])
        if artifact["groupId"] == args.include_group_id and "pom" not in artifact.get("types", [])
    }
    dependencies = [
        dependency
        for dependency in data.get("dependencies", [])
        if dependency["from"] in artifacts and dependency["to"] in artifacts
    ]
    dependencies.sort(
        key=lambda dependency: (
            str(artifacts[dependency["from"]]["artifactId"]),
            str(artifacts[dependency["to"]]["artifactId"]),
        )
    )

    adjacency: dict[str, set[str]] = defaultdict(set)
    reverse_adjacency: dict[str, set[str]] = defaultdict(set)
    for artifact_id in artifacts:
        adjacency.setdefault(artifact_id, set())
        reverse_adjacency.setdefault(artifact_id, set())
    for dependency in dependencies:
        adjacency[dependency["from"]].add(dependency["to"])
        reverse_adjacency[dependency["to"]].add(dependency["from"])

    summary_rows = []
    for artifact_id, artifact in sorted(
        artifacts.items(),
        key=lambda item: str(item[1]["artifactId"]),
    ):
        transitive_out = reachable_nodes(artifact_id, adjacency)
        transitive_in = reachable_nodes(artifact_id, reverse_adjacency)
        summary_rows.append(
            [
                artifact["artifactId"],
                len(adjacency[artifact_id]),
                len(reverse_adjacency[artifact_id]),
                len(transitive_out),
                len(transitive_in),
            ]
        )

    strongest_source = "-"
    strongest_out = 0
    for module_name, direct_out, _, _, _ in summary_rows:
        if direct_out > strongest_out:
            strongest_source = str(module_name)
            strongest_out = int(direct_out)

    edge_rows = []
    for dependency in dependencies:
        source = artifacts[dependency["from"]]
        target = artifacts[dependency["to"]]
        edge_rows.append(
            [
                source["artifactId"],
                target["artifactId"],
                dependency.get("resolution", ""),
            ]
        )

    dot_lines = [
        'digraph "module_dependencies" {',
        "  rankdir=LR;",
        '  graph [bgcolor="transparent", pad="0.2"];',
        '  node [shape=box, style="rounded,filled", color="#475569", fontname="Helvetica"];',
        '  edge [color="#64748b", arrowsize="0.8"];',
    ]
    for artifact_id, artifact in sorted(
        artifacts.items(),
        key=lambda item: str(item[1]["artifactId"]),
    ):
        direct_out = len(adjacency[artifact_id])
        direct_in = len(reverse_adjacency[artifact_id])
        total_degree = direct_out + direct_in
        label = f"{artifact['artifactId']}\\nout:{direct_out} in:{direct_in}"
        dot_lines.append(
            (
                f'  "{dot_text(artifact_id)}" [label="{dot_text(label)}", '
                f'fillcolor="{degree_color(total_degree)}"];'
            )
        )
    for dependency in dependencies:
        dot_lines.append(
            f'  "{dot_text(dependency["from"])}" -> "{dot_text(dependency["to"])}";'
        )
    dot_lines.append("}")

    cards = summary_cards(
        [
            ("Modules in graph", len(artifacts)),
            ("Direct module edges", len(dependencies)),
            (
                "Highest direct fan-out",
                f"{strongest_source} ({strongest_out})" if summary_rows else "-",
            ),
        ]
    )
    tables = [
        cards,
        (
            '<section class="panel"><h2>How Strength Is Estimated</h2>'
            "<p class=\"note\">Maven does not expose a weighted coupling score for reactor modules. "
            "This report uses direct incoming and outgoing dependency counts, plus transitive reach, "
            "as a practical proxy for dependency strength.</p></section>"
        ),
        table_section(
            "Direct Module Dependencies",
            ["From", "To", "Resolution"],
            edge_rows,
        ),
        table_section(
            "Coupling Summary",
            [
                "Module",
                "Direct Out",
                "Direct In",
                "Transitive Out",
                "Transitive In",
            ],
            summary_rows,
        ),
    ]

    intro = (
        "This page focuses on direct compile-scope dependencies between modules in the reactor. "
        "The graph highlights the structural connections, while the tables provide a simple coupling summary."
    )
    return (
        "\n".join(dot_lines),
        intro,
        "Compile-Scope Module Graph",
        Path("depgraph/reactor-dependencies.png"),
        tables,
    )


def main() -> int:
    args = parse_args()
    data = json.loads(Path(args.json_path).read_text(encoding="utf-8"))

    if args.mode == "engine":
        if not args.root_artifact:
            raise ValueError("--root-artifact is required in engine mode.")
        dot_content, intro, image_title, image_path, sections = build_engine_report(args, data)
    else:
        dot_content, intro, image_title, image_path, sections = build_reactor_report(args, data)

    dot_path = Path(args.dot_path)
    html_path = Path(args.html_path)
    png_path = Path(args.png_path)

    write_text(dot_path, dot_content)
    image_generated = render_png(dot_path, png_path)
    html_content = html_page(
        args.page_title,
        intro,
        image_section(image_title, image_path, image_generated),
        sections,
    )
    write_text(html_path, html_content)
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as exc:  # pragma: no cover - site generation should fail loudly.
        print(f"Failed to generate dependency site report: {exc}", file=sys.stderr)
        raise
