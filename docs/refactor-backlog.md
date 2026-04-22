# Refactor backlog

## Open Items

### Strategic epics

[ ] 79 Replace live-segment write path with range-partitioned ingest (Risk: HIGH)
    - Replace direct `Segment.put()/flush()/split` write admission with
      `active mutable -> immutable queue -> background drain -> stable publish`.
    - Keep `put()/get()/delete()` immediate visibility semantics and WAL-based
      crash recovery.
    - Reuse `KeyToSegmentMap` / `index.map` as the only persisted routing
      metadata in v1; route tables and partition queues stay runtime-only and
      are rebuilt from `index.map` + WAL replay on open.
    - Keep `segment` as the stable storage/publish backend in v1; stop routing
      user writes directly into live segments.
    - Allow breaking cleanup in `IndexConfiguration`, runtime tuning keys,
      metrics docs, and benchmark/test expectations.

[ ] 79.1 Freeze architecture, docs, and migration contract (Risk: HIGH)
[ ] 79.2 Introduce partition runtime and routing layer (Risk: HIGH)
[ ] 79.3 Switch `SegmentIndexImpl` read/write/delete paths to partitions (Risk: HIGH)
[ ] 79.4 Implement drain, publish, flush, close, and WAL recovery (Risk: HIGH)
[ ] 79.6 Clean up config, metrics, control-plane tuning, and obsolete code (Risk: HIGH)
[ ] 79.7 Refresh unit tests, integration tests, and JMH gates (Risk: HIGH)

[ ] 78 Monitoring/Management platform rollout (Risk: HIGH)

### Other open items

[ ] 54 Dedicated executor for index async ops (Risk: MEDIUM)
[ ] 55 Replace busy spin loops with retry + jitter (Risk: MEDIUM)
[ ] 56 Key-to-segment map read contention reduction (Risk: MEDIUM)
[ ] 57 Streaming iterators without full materialization (Risk: MEDIUM)
[ ] 5 Stop materializing merged cache lists on read (Risk: MEDIUM)
[ ] 6 Stream compaction without full cache snapshot (Risk: MEDIUM)
[ ] 7 Stream split without full cache snapshot (Risk: MEDIUM)
[ ] 8 Avoid full materialization in `IndexInternalConcurrent.getStream` (Risk: MEDIUM)
[ ] 9 Add eviction for heavy segment resources (Risk: MEDIUM)
[ ] 10 Allow cache shrink after peaks (Risk: LOW)
[ ] 13 Implement a real registry lock (Risk: MEDIUM)
[ ] 16 Replace busy-spin loops with retry+backoff+timeout (Risk: MEDIUM)
[ ] 17 Stop returning `null` on CLOSED in `SegmentIndexImpl.get` (Risk: MEDIUM)
[ ] 19 Propagate MDC context to stream consumption (Risk: LOW)
[ ] 42 Revisit `SegmentAsyncExecutor` rejection policy (Risk: MEDIUM)
[ ] 43 Replace registry close polling with completion signal (Risk: MEDIUM)
[ ] 44 Normalize split close/eviction flow (Risk: MEDIUM)
[ ] 46 Align iterator isolation naming and semantics (Risk: LOW)
[ ] 47 Consolidate BUSY/CLOSED retry loops (Risk: LOW)
[ ] 48 Test executor saturation and backpressure paths (Risk: MEDIUM)
[ ] 49 Test close path interactions (Risk: MEDIUM)
[ ] 50 Test split failure cleanup (Risk: MEDIUM)
[ ] 51 Test maintenance failure transitions (Risk: MEDIUM)

### Maintenance

[ ] M37 Audit `segment` package for unused or test-only code (Risk: LOW)
[ ] M38 Review `segment` package for test and Javadoc coverage (Risk: LOW)
[ ] M39 Audit `segmentindex` package for unused or test-only code (Risk: LOW)
[ ] M40 Review `segmentindex` package for test and Javadoc coverage (Risk: LOW)
[ ] M41 Audit `segmentregistry` package for unused or test-only code (Risk: LOW)
[ ] M42 Review `segmentregistry` package for test and Javadoc coverage (Risk: LOW)

## Done (Archive)

[x] 81 Replace technical `segmentindex.core` package vocabulary with domain ownership packages (Risk: HIGH)
[x] 81.1 Freeze the target package model and migration rules (Risk: HIGH)
[x] 81.2 Move root/session ownership into `core.session` (Risk: HIGH)
[x] 81.3 Collapse `core.lifecycle` into `core.session` (Risk: HIGH)
[x] 81.4 Move state types under `core.session.state` (Risk: MEDIUM)
[x] 81.5 Merge `core.operation` and `core.split` into `core.routing` (Risk: HIGH)
[x] 81.6 Move durability and storage integrity into `core.storage` (Risk: HIGH)
[x] 81.7 Dissolve `core.runtime` into session, routing, storage, and maintenance (Risk: HIGH)
[x] 81.8 Dissolve `core.infrastructure` into owning domains (Risk: MEDIUM)
[x] 81.9 Replace `core.observability` with `core.metrics` (Risk: MEDIUM)
[x] 81.10 Remove `core.internal` and `core.facade` as final package names (Risk: MEDIUM)
[x] 81.11 Collapse leftover assembly and access vocabulary after package moves (Risk: HIGH)
[x] 81.12 Finish when `segmentindex.core` reads as domain boundaries, not framework plumbing (Risk: MEDIUM)
