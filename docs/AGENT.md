# Documentation Agent Rules

These instructions apply to changes under `docs/`.

## Scope

- architecture pages in `docs/architecture`
- configuration pages in `docs/configuration`
- development pages in `docs/development`
- operations pages in `docs/operations`
- shared docs assets such as images and PlantUML sources under `docs/**/images`

## Core Rules

- Keep one page focused on one responsibility.
- Update docs in the same change when behavior or contracts change.
- Prefer exact terminology used in code over vague prose.
- If a page is a proposal, state that explicitly.
- Keep architecture pages as source-of-truth documents, not changelog notes.

## File Placement

- Put new architecture pages into the nearest existing section:
  - `docs/architecture/segmentindex`
  - `docs/architecture/segment`
  - `docs/architecture/registry`
  - `docs/architecture/general`
- Put diagrams next to their owning page under an `images/` directory.
- Use stable descriptive names.

Recommended naming:

- page: `kebab-case-topic.md`
- PlantUML source: `images/topic-name.plantuml`
- generated diagram: `images/topic-name.png`

## Diagram Rules

When a page uses PlantUML:

1. keep the `.plantuml` source checked in
2. keep the generated `.png` checked in next to it
3. embed the `.png` inline in the page
4. Do not add link to to the `.plantuml` source

On this workstation, use the `rp` command to generate the `.png` file from a
PlantUML source file. Prefer `rp` over invoking other rendering commands
directly.

Preferred Markdown pattern:

```md
![Short diagram description](images/example-diagram.png)
```

## Navigation Rules

If a page should be part of the published site:

- add it to `mkdocs.yml`
- add it to the nearest local index page such as
  `docs/architecture/segmentindex/index.md`

If a page is intentionally excluded from navigation, make that a conscious
choice.

## Writing Style

- Start with the purpose of the page.
- Describe invariants and contracts before implementation details.
- Use short lists for flows, rules, and responsibilities.
- Prefer exact class names and package names when mapping design to code.
- For redesign pages, clearly separate:
  - what stays
  - what changes
  - what is newly introduced

## Architecture-Specific Rules

- Keep `docs/architecture/segmentindex` focused on routing, concurrency,
  buffering, and orchestration.
- Keep `docs/architecture/segment` focused on stable segment internals and
  on-disk behavior.
- When a new layer is introduced above `segment`, describe package boundaries
  explicitly.
- For concurrency topics, always call out:
  - what blocks
  - what is immutable
  - what is retried
  - what is bounded

## Before Finishing

- verify links are relative and render in MkDocs
- verify images render inline
- verify both `.plantuml` and `.png` exist when diagrams are used
- verify `mkdocs.yml` and local index pages are updated when needed
- run `mkdocs build`
