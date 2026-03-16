#!/usr/bin/env python3

from __future__ import annotations

import re
import sys
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parent.parent
DOCS_DIR = REPO_ROOT / "docs"
MKDOCS_CONFIG = REPO_ROOT / "mkdocs.yml"
EXCLUDE_FILE = REPO_ROOT / "docs-nav-exclude.txt"


def load_nav_references() -> set[str]:
    text = MKDOCS_CONFIG.read_text(encoding="utf-8")
    return set(re.findall(r"([A-Za-z0-9_./-]+\.md)", text))


def load_exclusions() -> set[str]:
    items: set[str] = set()
    for raw_line in EXCLUDE_FILE.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#"):
            continue
        items.add(line)
    return items


def main() -> int:
    nav_refs = load_nav_references()
    exclusions = load_exclusions()
    docs_pages = sorted(str(path.relative_to(DOCS_DIR)) for path in DOCS_DIR.rglob("*.md"))

    missing = [page for page in docs_pages if page not in nav_refs and page not in exclusions]
    stale_exclusions = [page for page in sorted(exclusions) if not (DOCS_DIR / page).exists()]

    if stale_exclusions:
        print("Stale docs exclusions:")
        for page in stale_exclusions:
            print(f"  - {page}")
        return 1

    if missing:
        print("Docs pages missing from mkdocs nav and docs-nav-exclude.txt:")
        for page in missing:
            print(f"  - {page}")
        return 1

    print(
        f"Docs navigation check passed: {len(docs_pages)} markdown pages, "
        f"{len(nav_refs)} nav references, {len(exclusions)} explicit exclusions."
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
