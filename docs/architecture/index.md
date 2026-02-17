# 🧭 Architecture

This section is organized by responsibility to keep related topics together and
to centralize segment internals in one place.

## Sections

- [General](general/index.md) — cross-cutting format, integrity, recovery,
  limits, package boundaries, and glossary.
- [SegmentIndex](segmentindex/index.md) — top-level index orchestration:
  read/write paths, caching, performance, and index concurrency.
- [Segment](segment/index.md) — central place for segment internals:
  file layout, delta cache, Bloom filter, sparse/scarce index, and segment
  lifecycle.
- [Registry](registry/index.md) — segment registry state machine, cache-entry
  model, and concurrent loading/unloading flows.
