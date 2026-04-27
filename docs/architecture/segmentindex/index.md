# SegmentIndex Architecture

This section describes the top-level index orchestration layer: how operations
are routed, cached, and executed across segments.

Segment internals are intentionally centralized in
[Segment Architecture](../segment/index.md) to avoid duplication.

`SegmentIndex` no longer uses the removed partition-overlay runtime. Writes are
routed directly to stable segments, and read-after-write is provided by
segment-local caches. Route-first split coordination remains a separate
concern above stable segments.

## Topics

- [Read Path](read-path.md) — request routing and lookup flow.
- [Write Path](write-path.md) — routed writes, flush, compaction, and split
  orchestration.
- [WAL Runtime](wal-runtime.md) — internal ownership boundaries for WAL
  append, recovery, metadata, retention, and durability policy.
- [Range-Partitioned Ingest](range-partitioned-ingest.md) — compatibility note
  describing what replaced the removed runtime model.
- [Range-Partitioned Ingest Implementation Notes](range-partitioned-ingest-implementation.md)
  — current direct-to-segment write and route-split sequencing notes.
- [Caching Strategy](caching.md) — index-level cache roles and sizing.
- [Cache LRU](cache-lru.md) — bounded LRU behavior and trade-offs.
- [Performance Model & Sizing](performance.md) — throughput/latency model and
  tuning knobs.
- [Metrics Snapshot](metrics-snapshot.md) — stable index telemetry contract.
- [Segment Index Concurrency](segment-index-concurrency.md) — index
  thread-safety and lifecycle behavior, including `OPENING`, `READY`,
  `CLOSING`, `CLOSED`, and `ERROR`.
- [Implementation](implementation.md) — layer map from `SegmentIndex` down to
  `Segment` internals and where to inspect each responsibility.
