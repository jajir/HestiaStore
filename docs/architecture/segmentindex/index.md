# 📚 SegmentIndex Architecture

This section describes the top-level index orchestration layer: how operations
are routed, cached, and executed across segments.

Segment internals are intentionally centralized in
[Segment Architecture](../segment/index.md) to avoid duplication.

## Topics

- [Read Path](read-path.md) — request routing and lookup flow.
- [Write Path](write-path.md) — buffering, flush, compaction, and split
  orchestration.
- [Caching Strategy](caching.md) — index-level cache roles and sizing.
- [Cache LRU](cache-lru.md) — bounded LRU behavior and trade-offs.
- [Performance Model & Sizing](performance.md) — throughput/latency model and
  tuning knobs.
- [Metrics Snapshot](metrics-snapshot.md) — stable index telemetry contract.
- [Segment Index Concurrency](segment-index-concurrency.md) — index
  thread-safety and lifecycle behavior.
