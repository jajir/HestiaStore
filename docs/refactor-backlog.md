# Refactor backlog

## Open Items

### To Solve

#### Point-operation hot-path cleanup

Items `100.1` through `100.3` and `100.5` are complete. Item `100.4` is
intentionally deferred from this sequence; item `100.6` is the final active
step.

- [ ] 100.6 Re-profile the accepted sequence and stop at the measured boundary
  (Risk: LOW). Owner: `benchmarks` and refactoring docs. Outcome: run the
  canonical SegmentIndex comparison and focused JFR profiles, record accepted,
  rejected, and still-deferred candidates, and update architecture docs only
  for behavior that actually changed. Validation: benchmark comparison reports,
  `python3 scripts/check_docs_nav.py`, `mkdocs build --strict`, and
  `git diff --check`.
  Status: blocked by the performance gate. The complete canonical profile now
  runs, but its one-fork comparison produced contradictory large deltas and
  several `worse` results. Two focused five-fork live-get A/B cycles retained
  overlapping throughput intervals and did not reproduce a stable whole-path
  allocation mean. Next safe action: rerun the same artifacts on a quiet,
  dedicated host before archiving this item.

[ ] 80. Make WAL durability explicit for non-fsync storage adapters so `SYNC` and `GROUP_SYNC` never claim guarantees that the active storage backend cannot provide.
[ ] 81. Split `WalRuntime` into focused writer, recovery, segment-catalog, and sync-policy responsibilities instead of keeping all lifecycle paths behind one shared monitor.
[ ] 82. Replace the slow generic `WalStorageDirectory` fallback with an explicit seekable/capability-aware storage path, or reject unsupported backends early.
[ ] 79.1 Freeze architecture, docs, and migration contract (Risk: HIGH)
[ ] 79.2 Introduce partition runtime and routing layer (Risk: HIGH)
[ ] 79.3 Switch `SegmentIndexSession` read/write/delete paths to partitions (Risk: HIGH)
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

## Unfinished Audit

- Deferred status-value reuse. Boundary: `engine` point-operation and route
  leasing results. Reason: exploratory JFR did not identify `OperationResult`
  or `RouteLeaseAttempt` as material allocation sources, and generic singleton
  reuse adds type-safety and semantic risk. Evidence still needed: a
  representative allocation profile with these constructors on a material hot
  stack. Item `100.6` JFR still did not show that evidence. Next safe action:
  revisit only after a new production-equivalent profile does.
- Deferred 100.4 sampled point-operation latency recording. Boundary: `engine`
  operation monitoring. Reason: explicitly excluded from the current
  refactoring sequence; operation counts and latency observations remain
  unchanged. Item `100.6` sampled latency recording in execution stacks but
  recorded no live-get monitor contention. Next safe action: reconsider only
  after a production profile identifies material contention.
- Deferred chunk-store cache redesign. Boundary:
  `engine` `LruChunkStoreCache`. Reason: the implementation has one synchronized
  access-ordered map, but the persisted cache-enabled profile recorded no
  contention events, and item `100.6` JFR did not make it material. Evidence
  still needed: representative blocked-time or duplicate same-page load
  measurements. Next safe action: shard or add minimal per-key single-flight
  only for the demonstrated failure mode.
- Canonical comparison gate remains blocked. Boundary: `benchmarks`
  `segment-index-pr-smoke`. The benchmark now completes for both baseline and
  candidate, but the one-fork report simultaneously marked all get paths
  `22.07%` to `45.53%` worse and hot-put/split-heavy paths roughly `47%` to
  `57%` better. Focused five-fork throughput intervals overlapped, and a
  reverse-order live-get repeat remained inconclusive. Evidence still needed:
  the same A/B artifacts on a quiet, dedicated host. Next safe action: rerun;
  do not tune code to these contradictory point estimates.
- Repeated explicit maintenance during background split population can time
  out. Boundary: multi-segment benchmark setup and the production
  `flushAndWait()`/split interaction. The failure reproduced at 8,192 and
  32,768 keys and with segment-cache limits from `3` through `256`; the stack
  timed out in `MappedSegmentMaintenanceService.awaitSegmentReady(...)` while
  reloading a closed segment. The read benchmark now performs one final settled
  flush so it can measure reads, but that does not fix the underlying
  maintenance interaction. Next safe action: investigate it as a dedicated
  lifecycle/concurrency issue outside item `100.6`.
- Route-lease monitor contention needs production confirmation. Boundary:
  `RouteEntry` lease acquisition/release. The item `100.6` JFR hot-put stress
  profile recorded 14 blocked monitor events totaling `143 ms` with twenty
  threads targeting one route; live get recorded none. Next safe action:
  redesign only if a production JFR shows the same monitor as material.
- Deferred immutable segment-ID set. Boundary: `RouteMapSnapshot`. Reason:
  direct map-value scanning removed the temporary list and improved the
  50,000-route lookup from `323.428 us/op` to `115.267 us/op` with effectively
  zero per-call allocation. Item `100.6` JFR did not show the remaining scan as
  material. Evidence still needed: an end-to-end exact-segment acquisition
  profile showing that it justifies duplicate snapshot state and extra
  publication cost.

## Done (Archive)

- [x] 100.5 Replace list-building segment membership checks with direct
  snapshot lookup (Risk: MEDIUM).
    - `RouteMapSnapshot.containsSegmentId(...)` now scans immutable map values
      directly, and `MappedSegmentLeaseService` uses it for exact mapped-segment
      checks without constructing an unbounded list.
    - A focused one-thread JMH benchmark measured a successful middle-position
      lookup with five forks, three warmup iterations, five measurement
      iterations, and GC profiling on the same JDK 25 arm64 macOS environment.
    - Results were `63.807 -> 5.627 ns/op` at 10 routes,
      `5.647 -> 1.649 us/op` at 1,000, `64.027 -> 22.364 us/op` at 10,000,
      `323.428 -> 115.267 us/op` at 50,000, and
      `660.687 -> 228.547 us/op` at 100,000. Reported confidence intervals did
      not overlap at any tested size.
    - Allocation changed from `384 B/op`, `4,344 B/op`, `40,361 B/op`,
      `200,346 B/op`, and `400,355 B/op`, respectively, to effectively
      zero. The improvement is measurable from 10 routes, the smallest tested
      size; no claim is made below 10.
    - Focused engine tests passed with 23 tests, benchmark contract/tooling tests
      passed with 12 tests, and `mvn clean verify` passed across all 10 reactor
      modules.
- [x] 100.3 Publish and reuse one immutable `RouteMapSnapshot` per route-map
  version (Risk: MEDIUM).
    - `PersistentSegmentRouteMap` now publishes copied routes and their version
      together through one volatile `RouteMapSnapshot` reference. Repeated
      `snapshot()` calls perform no locking or wrapper allocation, and only real
      mutations publish the next instance.
    - Focused identity, no-op mutation, version-transition, and concurrent split
      publication coverage passed: 17 route-map tests. `mvn clean verify`
      passed across all 10 reactor modules.
    - Five-fork live-get JMH with GC data moved from
      `3.493 M ops/s`, `168.175 B/op` to
      `3.427 M ops/s`, `148.507 B/op`. Throughput changed by `-1.87%` with
      overlapping confidence intervals; allocation fell by `19.668 B/op`
      (`-11.70%`) with non-overlapping reported confidence intervals.
    - Five-fork hot-put throughput moved from `2.853 M ops/s` to
      `3.170 M ops/s`, but its confidence intervals overlapped. Allocation was
      bimodal across the candidate and repeat runs (`136` to `216 B/op`), so no
      hot-put allocation or throughput improvement is claimed.
    - The focused throughput summaries passed the canonical comparison tool
      with no regression: live get was `neutral` and hot put was `better` under
      its percentage thresholds. The broader canonical-profile setup failure is
      retained in the unfinished audit rather than hidden.
- [x] 100.2 Add production-equivalent context-logging benchmark coverage
  (Risk: LOW).
    - `SegmentIndexGetBenchmark` and `SegmentIndexHotRoutePutBenchmark` now
      expose the same enabled/disabled context-logging parameter. Existing
      canonical profiles pin it off, while
      `segment-index-context-logging.json` runs the focused comparison with a
      Logback MDC backend and log emission disabled.
    - The profile runner now rejects empty JMH result files instead of
      publishing a successful summary after all forks fail.
    - Benchmark profile contract and script smoke coverage passed with 11
      tests; the packaged runner also passed a direct enabled-MDC smoke run.
      `mvn clean verify` passed across all 10 reactor modules.
    - Three forks with three warmup and five measurement iterations reported
      live-get means of `3.487 M ops/s`, `171.884 B/op` disabled and
      `3.122 M ops/s`, `204.007 B/op` enabled. Hot-put means were
      `2.527 M ops/s`, `201.239 B/op` disabled and `2.800 M ops/s`,
      `237.404 B/op` enabled.
    - Enabled-MDC allocation means were about `32.123 B/op` higher for live
      gets and `36.165 B/op` higher for hot puts. Throughput and allocation
      confidence intervals overlapped in both comparisons, so this run alone
      does not establish either effect. Production logging behavior was
      unchanged.
- [x] 100.1 Close the losing `SegmentReadPath` searcher after concurrent first
  access (Risk: MEDIUM).
    - `SegmentReadPath` now closes a redundantly constructed searcher
      immediately after losing the cache CAS; the winner remains cached.
    - A deterministic two-thread `SegmentReadPathTest` verifies that exactly
      one supplier closes after the race and the winner closes with the read
      path.
    - `mvn -pl engine -Dtest=SegmentReadPathTest test` passed with 7 tests.
    - `mvn clean verify` passed across all 10 reactor modules.
[x] 99. Replace `SessionOperationGate` per-operation monitor contention with atomic in-flight tracking and bounded close-side drain polling (Risk: MEDIUM)
    - Focused session concurrency tests, benchmark module tests, and the full
      Maven verification pipeline pass.
    - Targeted JMH stack sampling removed all `SessionOperationGate` blocked
      frames and the per-operation `Object.notifyAll()` frame.
    - The targeted throughput result moved by -3.3%, with heavily overlapping
      confidence intervals; the canonical smoke profile was also too noisy to
      establish a throughput regression or improvement.
[x] 97. Clarify `core.session` responsibility boundaries by moving topology runtime composition to route/topology internals, giving core storage its own open spec and observer types, and preserving lifecycle behavior.
[x] 98. Simplify SegmentIndex initialization after the broad assembly refactor by keeping explicit opening points for bootstrap, session startup, and runtime resources while removing pattern-only assembler/request/components layers.
[x] 89. Rework split routing around a runtime `RouteTopology` so route handoff, draining, and split publish are owned by topology code while `SegmentRegistry` remains responsible only for physical segment instances and `SegmentRouteMap` remains responsible only for persisted routing.
[x] 90. Define the `RouteTopology` contract with route states such as `ACTIVE`, `DRAINING`, and `RETIRED`, plus `RouteLease` acquisition/release semantics and deterministic drain behavior for in-flight routed operations.
[x] 91. Add topology bootstrap from the versioned `SegmentRouteMap` snapshot so startup builds runtime route entries without changing `SegmentRegistry`, `BlockingSegment`, or `Segment` contracts.
[x] 92. Refactor foreground routed operations to resolve a `SegmentRouteMap` snapshot, acquire a `RouteTopology` lease for the resolved segment id and map version, use the existing `SegmentRegistry` to load the segment, and retry from the correct boundary on topology drain, stale version, registry unavailability, segment `BUSY`, or segment `CLOSED`.
[x] 93. Add focused tests for topology lease acquire/release, drain waiting, stale map-version rejection, route retirement, and the retry boundaries used by `put`, `delete` through tombstone writes, and `get`.
[x] 94. Rework split execution so the parent route moves to `DRAINING` before child materialization, in-flight leases drain, child materialization uses existing registry materialization, child routes publish in `RouteTopology`, `SegmentRouteMap` is updated and flushed, and the retired parent segment is cleaned up after publish.
[x] 95. Define and test the split failure policy for topology publish, `SegmentRouteMap` persistence failure, child materialization cleanup, parent cleanup retry, and startup recovery from the persisted map.
[x] 96. Remove the legacy split admission gate after routed operations and split publish use `RouteTopology` leases, then update concurrency documentation to make `RouteTopology`, `SegmentRegistry`, and `SegmentRouteMap` ownership boundaries explicit.
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
      `SegmentIndexWritePathMetrics` model.
    - End-game achieved: legacy runtime configuration and partition/drain
      compatibility surfaces were removed from the SegmentIndex API and
      monitoring payloads.

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
[x] 81.9 Replace `core.observability` with `segmentindex.monitoring` (Risk: MEDIUM)
[x] 81.10 Remove `core.internal` and `core.facade` as final package names (Risk: MEDIUM)
[x] 81.11 Collapse leftover assembly and access vocabulary after package moves (Risk: HIGH)
[x] 81.12 Finish when `segmentindex.core` reads as domain boundaries, not framework plumbing (Risk: MEDIUM)
