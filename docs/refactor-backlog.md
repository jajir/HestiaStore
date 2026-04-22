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

### Maintenance

[ ] M37 Audit `segment` package for unused or test-only code (Risk: LOW)
[ ] M38 Review `segment` package for test and Javadoc coverage (Risk: LOW)
[ ] M39 Audit `segmentindex` package for unused or test-only code (Risk: LOW)
[ ] M40 Review `segmentindex` package for test and Javadoc coverage (Risk: LOW)
[ ] M41 Audit `segmentregistry` package for unused or test-only code (Risk: LOW)
[ ] M42 Review `segmentregistry` package for test and Javadoc coverage (Risk: LOW)

## Done (Archive)

[x] 87 Decompose WAL into smaller durable-log subsystems (Risk: HIGH)
    - End-game achieved: `WalRuntime` now reads as a compatibility-facing
      orchestration facade instead of a monolithic durable-log implementation.
    - End-game achieved: WAL metadata/catalog, segment inventory, recovery,
      append-path writing, durability policy, and metrics ownership now live in
      dedicated collaborators under `segmentindex.wal`.
    - End-game achieved: public `WalRuntime` operations, metrics, and WAL
      on-disk compatibility remain stable while internal ownership boundaries
      are explicit and test-covered.
[x] 87.1 Freeze `WalRuntime` contract, state model, and migration invariants (Risk: HIGH)
[x] 87.2 Add characterization coverage for the current WAL orchestration seams (Risk: HIGH)
[x] 87.3 Extract WAL metadata/catalog state behind one internal model (Risk: HIGH)
[x] 87.4 Extract segment inventory, retention, and cleanup ownership (Risk: HIGH)
[x] 87.5 Extract recovery scan and corruption-repair ownership (Risk: HIGH)
[x] 87.6 Extract append writer and sync policy execution (Risk: HIGH)
[x] 87.7 Reduce `WalRuntime` to orchestration and lifecycle assembly (Risk: HIGH)
[x] 87.8 Refresh docs, benchmarks, and follow-on cleanup after extraction (Risk: HIGH)

[x] 83 Replace compatibility-shaped public model with domain-shaped public API (Risk: HIGH)
    - End-game achieved: canonical public configuration now exposes
      direct-to-segment write-path vocabulary via `IndexWritePathConfiguration`
      instead of making legacy partition naming the source of truth.
    - End-game achieved: canonical runtime metrics now expose a dedicated
      `SegmentIndexWritePathMetrics` model while legacy
      `partition`/`drain`-named metrics are isolated behind a compatibility
      view.
    - End-game achieved: legacy accessors remain available only as deprecated
      compatibility shims and the REST/JSON bridge reads them from the
      compatibility boundary instead of the main runtime model.

[x] 82 Collapse split scheduling into a dedicated planner package (Risk: HIGH)
    - End-game achieved: `org.hestiastore.index.segmentindex.core.splitplanner`
      is now the control-plane package for split hint intake, periodic
      reconciliation, candidate selection, and admission into the split worker
      pool.
    - End-game achieved: split execution stays separate from planning. The
      planner owns "what should be scheduled"; split workers own "execute this
      already admitted split".
    - End-game achieved: planner is the only place allowed to submit split
      work. Write paths, maintenance finalization, and timer ticks emit only
      hints or rescan requests.
    - End-game achieved: thread topology is reduced to one planner thread plus
      one split worker pool, while route publish exclusivity, retry behavior,
      and candidate deduplication remain explicit and test-covered.

[x] 82.1 Freeze target split-planner architecture and migration invariants (Risk: HIGH)
[x] 82.2 Add characterization tests for current split trigger and admission behavior (Risk: HIGH)
[x] 82.3 Introduce `core.splitplanner` package with behavior-preserving type moves (Risk: MEDIUM)
[x] 82.4 Extract explicit planner state and planner-facing API (Risk: MEDIUM)
[x] 82.5 Introduce a single split-task dispatch seam (Risk: HIGH)
[x] 82.6 Route all split triggers through planner requests only (Risk: HIGH)
[x] 82.7 Separate candidate discovery from split execution in code and tests (Risk: HIGH)
[x] 82.8 Replace dual policy executors with a single planner thread model (Risk: HIGH)
[x] 82.9 Simplify executor topology and runtime assembly (Risk: HIGH)
[x] 82.10 Remove obsolete policy vocabulary and close the migration (Risk: MEDIUM)
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
