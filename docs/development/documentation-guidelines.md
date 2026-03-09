# Documentation Guidelines

This page defines lightweight rules for updating HestiaStore documentation.
Use it together with [Documentation](documentation.md), which explains how to
build and publish the site.

## Scope

These rules apply to:

- architecture pages in `docs/architecture`
- configuration and operations pages in `docs/configuration` and
  `docs/operations`
- development-facing pages in `docs/development`

## Basic Rules

- Keep one page focused on one responsibility.
- Update docs in the same change when behavior or contracts change.
- Prefer concrete terminology used in code over vague prose.
- If code and docs disagree, either fix both or explicitly mark the page as a
  proposal.
- Keep architecture pages as source-of-truth documents, not changelog-style
  notes.

## File Placement

- Put new architecture pages under the closest existing section:
  - `docs/architecture/segmentindex` for index orchestration
  - `docs/architecture/segment` for segment internals
  - `docs/architecture/registry` for registry lifecycle and cache behavior
  - `docs/architecture/general` for cross-cutting topics
- Put diagrams next to their owning page under an `images/` directory.
- Keep image and diagram file names stable and descriptive.

Recommended naming:

- page: `kebab-case-topic.md`
- PlantUML source: `images/topic-name.plantuml`
- generated image: `images/topic-name.png`

## Diagrams

When a page uses a PlantUML diagram:

1. keep the `.plantuml` source checked in
2. keep the generated `.png` checked in next to it
3. embed the `.png` in the page
4. add a nearby link to the `.plantuml` source

Preferred Markdown pattern:

```md
![Short diagram description](images/example-diagram.png)

PlantUML source:
[`docs/path/to/example-diagram.plantuml`](images/example-diagram.plantuml)
```

Do not leave only a text link to the PNG when the page is meant to display the
diagram inline.

## Navigation

If a page is intended to be part of the published site:

- add it to `mkdocs.yml`
- add it to the relevant local section index page, for example
  `docs/architecture/segmentindex/index.md`

If a page is intentionally not part of the published nav, make that a
conscious choice.

## Writing Style

- Start with the purpose of the page.
- Describe invariants and contracts before implementation details.
- Use short lists for rules, flows, and responsibilities.
- Prefer exact class/package names when mapping design to code.
- When proposing a redesign, clearly mark what stays, what changes, and what
  is newly introduced.

## Change Checklist

Before finishing a documentation change, verify:

- the page is in the correct section
- links are relative and render in MkDocs
- images render inline
- PlantUML source and PNG are both present
- `mkdocs.yml` includes the page when needed
- local section index links to the page when needed
- `mkdocs build` succeeds

## Architecture-Specific Guidance

- Keep `docs/architecture/segmentindex` focused on top-level routing,
  concurrency, buffering, and orchestration.
- Keep `docs/architecture/segment` focused on stable segment internals and
  on-disk behavior.
- When a new layer is introduced above `segment`, describe package boundaries
  explicitly.
- For concurrency topics, always call out:
  - what blocks
  - what is immutable
  - what is retried
  - what is bounded

