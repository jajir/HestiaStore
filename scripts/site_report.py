#!/usr/bin/env python3
"""Shared helpers for generated Maven site reports."""

from __future__ import annotations

import html
import shutil
import subprocess
from pathlib import Path


def html_text(value: object) -> str:
    return html.escape(str(value))


def dot_text(value: object) -> str:
    return str(value).replace("\\", "\\\\").replace('"', '\\"')


def write_text(path: Path, content: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content, encoding="utf-8")


def render_dot_png(dot_path: Path, png_path: Path) -> bool:
    dot_executable = shutil.which("dot")
    if not dot_executable:
        return False

    png_path.parent.mkdir(parents=True, exist_ok=True)
    subprocess.run(
        [dot_executable, "-Tpng", str(dot_path), "-o", str(png_path)],
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


def table_section(
    title: str, headers: list[str], rows: list[list[object]]
) -> str:
    header_html = "".join(f"<th>{html_text(header)}</th>" for header in headers)
    body_rows = []
    for row in rows:
        columns = "".join(f"<td>{html_text(column)}</td>" for column in row)
        body_rows.append(f"<tr>{columns}</tr>")
    rows_html = (
        "\n".join(body_rows)
        if body_rows
        else '<tr><td colspan="99">No rows.</td></tr>'
    )
    return (
        f'<section class="panel"><h2>{html_text(title)}</h2><table>'
        f"<thead><tr>{header_html}</tr></thead><tbody>{rows_html}</tbody>"
        "</table></section>"
    )


def html_page(
    title: str, intro: str, image_html: str, sections: list[str]
) -> str:
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
      background: var(--bg);
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
      border-radius: 8px;
      background: var(--panel);
      box-shadow: 0 8px 24px rgba(15, 23, 42, 0.06);
    }}
    .cards {{
      display: grid;
      grid-template-columns: repeat(auto-fit, minmax(180px, 1fr));
      gap: 1rem;
      margin-top: 1.25rem;
    }}
    .card {{
      padding: 1rem;
      border-radius: 8px;
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
    img {{
      display: block;
      width: 100%;
      height: auto;
      border-radius: 8px;
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
