# 🧩 General Architecture

Cross-cutting architecture topics that apply to the whole engine (not to one
component only).

## Topics

- [Data Block Format](datablock.md) — low-level block and chunk structure.
- [Filters & Integrity](filters.md) — chunk filter pipeline and validation.
- [Chain of Filters](chain-of-filters.md) — shared filter-chain helper.
- [Concurrency Model](concurrency.md) — index-wide synchronization model.
- [Consistency & Recovery](recovery.md) — crash-safety and recovery model.
- [Package layout](package-boundaries.md) — module/package layout and dependency contracts.
- [Limitations & Trade-offs](limits.md) — current constraints and risks.
- [Glossary](glossary.md) — shared terminology.

Monitoring docs were moved to a dedicated section:
[Architecture / Monitoring](../monitoring/index.md).
