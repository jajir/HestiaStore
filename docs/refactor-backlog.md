# Refactor backlog

## Open Items

### To Solve

[ ] 80. Make WAL durability explicit for non-fsync storage adapters so `SYNC` and `GROUP_SYNC` never claim guarantees that the active storage backend cannot provide.
[ ] 81. Split `WalRuntime` into focused writer, recovery, segment-catalog, and sync-policy responsibilities instead of keeping all lifecycle paths behind one shared monitor.
[ ] 82. Replace the slow generic `WalStorageDirectory` fallback with an explicit seekable/capability-aware storage path, or reject unsupported backends early.
[ ] 79.1 Freeze architecture, docs, and migration contract (Risk: HIGH)
[ ] 79.2 Introduce partition runtime and routing layer (Risk: HIGH)
[ ] 79.3 Switch `SegmentIndexImpl` read/write/delete paths to partitions (Risk: HIGH)
[ ] 79.4 Implement drain, publish, flush, close, and WAL recovery (Risk: HIGH)
[ ] 79.6 Clean up config, metrics, control-plane tuning, and obsolete code (Risk: HIGH)
[ ] 79.7 Refresh unit tests, integration tests, and JMH gates (Risk: HIGH)
[ ] 78 Monitoring/Management platform rollout (Risk: HIGH)

### Regular Maintenance

[ ] M37 Audit `segment` package for unused or test-only code (Risk: LOW)
[ ] M38 Review `segment` package for test and Javadoc coverage (Risk: LOW)
[ ] M39 Audit `segmentindex` package for unused or test-only code (Risk: LOW)
[ ] M40 Review `segmentindex` package for test and Javadoc coverage (Risk: LOW)
[ ] M41 Audit `segmentregistry` package for unused or test-only code (Risk: LOW)
[ ] M42 Review `segmentregistry` package for test and Javadoc coverage (Risk: LOW)
[ ] M43 Avoid pattern-name abstractions unless they own lifecycle, resource opening, or rollback-sensitive cleanup (Risk: LOW)

## Done (Archive)

[x] 97. Clarify `core.session` responsibility boundaries by moving topology runtime composition to `core.topology`, giving core storage its own open spec and observer types, and preserving lifecycle behavior.
[x] 98. Simplify SegmentIndex initialization after the broad assembly refactor by keeping explicit opening points for bootstrap, session startup, and runtime resources while removing pattern-only assembler/request/components layers.
[x] 89. Rework split routing around a runtime `SegmentTopology` so route handoff, draining, and split publish are owned by topology code while `SegmentRegistry` remains responsible only for physical segment instances and `KeyToSegmentMap` remains responsible only for persisted routing.
[x] 90. Define the `SegmentTopology` contract with route states such as `ACTIVE`, `DRAINING`, and `RETIRED`, plus `RouteLease` acquisition/release semantics and deterministic drain behavior for in-flight routed operations.
[x] 91. Add topology bootstrap from the versioned `KeyToSegmentMap` snapshot so startup builds runtime route entries without changing `SegmentRegistry`, `BlockingSegment`, or `Segment` contracts.
[x] 92. Refactor foreground routed operations to resolve a `KeyToSegmentMap` snapshot, acquire a `SegmentTopology` lease for the resolved segment id and map version, use the existing `SegmentRegistry` to load the segment, and retry from the correct boundary on topology drain, stale version, registry unavailability, segment `BUSY`, or segment `CLOSED`.
[x] 93. Add focused tests for topology lease acquire/release, drain waiting, stale map-version rejection, route retirement, and the retry boundaries used by `put`, `delete` through tombstone writes, and `get`.
[x] 94. Rework split execution so the parent route moves to `DRAINING` before child materialization, in-flight leases drain, child materialization uses existing registry materialization, child routes publish in `SegmentTopology`, `KeyToSegmentMap` is updated and flushed, and the retired parent segment is cleaned up after publish.
[x] 95. Define and test the split failure policy for topology publish, `KeyToSegmentMap` persistence failure, child materialization cleanup, parent cleanup retry, and startup recovery from the persisted map.
[x] 96. Remove the legacy split admission gate after routed operations and split publish use `SegmentTopology` leases, then update concurrency documentation to make `SegmentTopology`, `SegmentRegistry`, and `KeyToSegmentMap` ownership boundaries explicit.
[x] 83. Define the new split runtime contract around `hintSplitCandidate(...)`, `awaitQuiescence(...)`, and managed lifecycle shutdown, and remove public scheduling concepts such as full-scan requests from the intended service shape.
[x] 84. Introduce a managed split runtime skeleton with explicit `OPENING -> RUNNING -> CLOSING -> CLOSED` state transitions and fail-fast behavior for calls made outside `RUNNING`.
[x] 85. Replace the current split-policy work-state loop with a candidate registry built from `Map<SegmentId, State>` plus a blocking ready queue so split hints are deduplicated and workers block instead of polling.
[x] 86. Split policy evaluation from split execution so policy workers only validate mapping and threshold eligibility, then hand off accepted candidates to the dedicated split executor.
[x] 87. Rebuild periodic reconciliation around the new candidate registry so the 250 ms scanner only offers over-threshold mapped segments that are not already queued or in process.
[x] 88. Rework split runtime tests around lifecycle, deduplicated candidate scheduling, blocking worker wakeup, quiescence, and close-drain behavior before removing the remaining legacy split-policy orchestration.
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
[x] 81.9 Replace `core.observability` with `segmentindex.metrics` (Risk: MEDIUM)
[x] 81.10 Remove `core.internal` and `core.facade` as final package names (Risk: MEDIUM)
[x] 81.11 Collapse leftover assembly and access vocabulary after package moves (Risk: HIGH)
[x] 81.12 Finish when `segmentindex.core` reads as domain boundaries, not framework plumbing (Risk: MEDIUM)
