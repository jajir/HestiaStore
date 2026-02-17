# 🧩 General Architecture

Cross-cutting architecture topics that apply to the whole engine (not to one
component only).

## Topics

- [Data Block Format](datablock.md) — low-level block and chunk structure.
- [Filters & Integrity](filters.md) — chunk filter pipeline and validation.
- [Chain of Filters](chain-of-filters.md) — shared filter-chain helper.
- [Concurrency Model](concurrency.md) — index-wide synchronization model.
- [Consistency & Recovery](recovery.md) — crash-safety and recovery model.
- [Monitoring Bridge](monitoring-bridge.md) — optional telemetry modules.
- [Package Boundaries](package-boundaries.md) — package dependency contracts.
- [Limitations & Trade-offs](limits.md) — current constraints and risks.
- [Glossary](glossary.md) — shared terminology.
