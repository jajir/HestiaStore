# 🧭 Architecture

This section explains how HestiaStore works under the hood: the core on‑disk structures, read/write data paths, caching and concurrency models, and the knobs that influence performance and reliability. Use these pages to reason about behavior, tune the system, and understand limits and trade‑offs.

- [Sparse Index](arch-index.md) — structure and sampling that bound seeks into the main SST for fast point lookups.
- [Segment Design](segment.md) — segment lifecycle, delta cache files, compaction, and when/why segments split.
- [Data Block Format](datablock.md) — chunked storage layout, block/cell sizes, and chunk headers.
- [Write Path](write-path.md) — API → buffer → flush to delta files → compaction/split; atomic commits with temp files and rename.
- [Read Path](read-path.md) — delta cache first, then Bloom + sparse index to a bounded on‑disk scan.
- [Consistency & Recovery](recovery.md) — crash‑safety model (no WAL), transactional writers, and check/repair steps.
- [Filters & Integrity](filters.md) — magic number, CRC32, Snappy, and XOR pipelines on write/read.
- [Caching Strategy](caching.md) — index write buffer, per‑segment caches (Bloom/scarce/delta), LRU, and warm‑up.
- [Performance Model & Sizing](performance.md) — key tuning knobs, I/O patterns, and practical recipes.
- [Concurrency](concurrency.md) — thread‑safe mode, per‑segment iteration safety, and process exclusivity.
- [On‑Disk Layout & File Names](on-disk-layout.md) — directory contents, file naming, and commit pattern.
- [Limitations & Trade‑offs](limits.md) — hard limits, anti‑patterns, and recommended mitigations.
- [Glossary](glossary.md) — concise definitions for terms used across the architecture.
