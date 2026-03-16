# Documentation Guide

This is the canonical maintainer guide for documentation work in `docs/`.
Agent-specific instructions should reference this page instead of duplicating
its rules.

## Documentation model

The published docs are organized around the external journey first:

- `Evaluate` for fit, quality, security, and benchmark context
- `Getting Started` for installation and first integration
- `Configuration` for builder-level settings
- `Operations` for WAL, monitoring, backup, and tuning
- `Architecture` for source-of-truth explanations
- `Contribute & Community` for maintainer and contributor workflows

Use a light Diataxis split:

- tutorial: guided learning path
- how-to: task-oriented operational or integration steps
- reference: API, configuration, or command facts
- explanation: architecture, trade-offs, and rationale

## Required content rules

- Keep one page focused on one responsibility.
- Update docs in the same change when behavior or contracts change.
- Prefer exact code terminology over vague prose.
- If code and docs disagree, fix both or clearly mark the page as a proposal.
- Keep architecture pages as source-of-truth documents, not changelog notes.
- Do not leave placeholder pages such as "content to be expanded" in published
  navigation.

## File placement

- `docs/architecture`: explanation and source-of-truth design material
- `docs/configuration`: builder settings and runtime configuration reference
- `docs/operations`: runbooks, monitoring, tuning, and backup guidance
- `docs/how-to-use`: installation and early integration path
- `docs/development`: contributor and maintainer material

Architecture placement rules:

- `docs/architecture/segmentindex` for routing, buffering, concurrency, and
  orchestration
- `docs/architecture/segment` for stable segment internals and on-disk behavior
- `docs/architecture/registry` for registry state and cache lifecycle
- `docs/architecture/general` for cross-cutting topics

## Navigation and exclusions

Every Markdown page under `docs/` must be one of:

- published in `mkdocs.yml`
- intentionally excluded in `docs-nav-exclude.txt`

Pages excluded from nav should be rare and should usually be one of:

- redirects or moved-page stubs
- internal agent instructions
- archived benchmark snapshots not intended for the main user journey

Run the navigation check before finishing a docs change:

```bash
python3 scripts/check_docs_nav.py
```

## Local preview and publishing

Preview docs locally:

```bash
mkdocs serve
```

Build the site exactly as CI does:

```bash
mkdocs build --strict
```

Deployment to `gh-pages` is handled by GitHub Actions. Do not rely on manual
deploy steps as the primary path.

## Writing style

- Start with the purpose of the page.
- Put invariants and contracts before implementation detail.
- Prefer short lists for flows, rules, and responsibilities.
- Use consistent title casing and avoid decorative emoji in headings.
- Prefer exact class and package names when mapping docs to code.
- For redesign pages, explicitly separate what stays, what changes, and what is
  newly introduced.

## Diagrams

When a page uses PlantUML:

1. keep the `.plantuml` source checked in
2. keep the generated `.png` checked in next to it
3. embed the `.png` inline in the page

Preferred pattern:

```md
![Short diagram description](images/example-diagram.png)
```

On this workstation, use `rp` to render PlantUML sources to PNG files.

## Page template

Use this lightweight starting shape when adding or rewriting a page:

```md
---
title: Page Title
audience: user | operator | contributor | evaluator
doc_type: tutorial | how-to | reference | explanation
owner: docs | engine | ops
---

# Page Title

One paragraph describing the page purpose.

## Main content

## Related docs
```

The front matter is optional for old pages, but new pages should include it
when practical.

## Review checklist

Before finishing a docs change:

- the page is in the right section
- links are relative and render in MkDocs
- images render inline
- PlantUML source and PNG both exist when diagrams are used
- `mkdocs.yml` includes published pages
- `docs-nav-exclude.txt` includes intentional exclusions
- the local section index links to the page when needed
- `python3 scripts/check_docs_nav.py` passes
- `mkdocs build --strict` passes

## Recurring maintenance checklist

Run this review when the docs structure changes significantly:

- remove stale examples and old package names
- remove placeholder pages or archive them
- check for orphaned docs pages
- check for broken links and missing images
- split oversized pages that mix multiple responsibilities
- confirm benchmark artifact pages are not used as primary docs
