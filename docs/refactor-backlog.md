# Refactor backlog

## Open Items

No open items.

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
[ ] 56 Key‑to‑segment map read contention reduction (Risk: MEDIUM)
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

[x] 80 Clarify `segmentindex.core` architecture seams (Risk: MEDIUM)
    - Closed after the `80.*` wave removed the major wrapper layers, split the
      runtime graph into storage/split/service steps, normalized package
      ownership, and reduced the remaining issues to local readability and
      naming.

[x] 80.26 Revisit eager executor creation strategy in `IndexExecutorRegistry` (Risk: MEDIUM)
    - Kept observed hot-path pools eager and moved support executors to lazy
      creation through `LazyExecutorReference`, so startup wiring is smaller
      without losing runtime monitoring or close-order guarantees.

[x] 80.30 Rename framework-style types to domain-style names where behavior is now stable (Risk: LOW)
    - Renamed the major misleading roots and bags, including
      `SegmentIndexCoreComposition -> SegmentIndexCoreGraph`,
      `SegmentIndexCoreAssemblyRequest -> SegmentIndexCoreInputs`,
      `SegmentIndexRuntimeAssemblyRequest -> SegmentIndexRuntimeInputs`,
      `SegmentIndexServiceRuntimeState -> SegmentIndexRuntimeServices`,
      `SegmentIndexStorageRuntimeState -> SegmentIndexRuntimeStorage`,
      `SegmentIndexSplitRuntimeState -> SegmentIndexRuntimeSplits`, and
      `SegmentIndexManagedIndexAssembler -> SegmentIndexManagedIndexFactory`.

[x] 80.30.a Rename the most misleading framework-style type names
    - Removed the largest remaining names that hid ownership or behavior.

[x] 80.30.b Rename the remaining framework-style type names
    - Finished the stable rename sweep once the structural seams stopped moving.

[x] 80.31 Normalize package boundaries between `runtime`, `lifecycle`, `maintenance`, and `control` (Risk: MEDIUM)
    - Moved classes by ownership rather than historical placement, including
      the whole split subsystem under `core.split`, lifecycle-owned MDC
      decorators under `core.lifecycle`, and facade/maintenance/consistency
      seams next to the packages that actually own them.

[x] 80.33 Consolidate stable-segment maintenance read/write helpers around one boundary (Risk: MEDIUM)
    - Collapsed stable-segment and split helper chains into a smaller set of
      real boundaries (`StableSegmentCoordinator`, `DirectSegmentCoordinator`,
      `StableSegmentGateway`, `DefaultSegmentMaterializationService`,
      `RouteSplitPublishCoordinator`).

[x] 80.34 Reduce factory chaining in runtime graph assembly (Risk: MEDIUM)
    - Closed with the current `core storage -> split infra -> services` shape
      and the removal of the remaining top-level assembly shells and pass-through
      bags.

[x] 80.35 Rework observability package count by deleting or merging low-value microtypes (Risk: MEDIUM)
    - Reduced `core.observability` to a smaller cohesive set of types
      (currently 11 Java classes), with counters, latency views, and snapshot
      glue folded into `Stats`, `SegmentIndexMetricsCollector`,
      `SegmentIndexMetricsSnapshotFactory`, and `StableSegmentRuntimeMetrics`.

[x] 80.36 Rework maintenance package count by merging one-use policy helpers (Risk: MEDIUM)
    - Collapsed the maintenance micro-helper graph into `BackgroundSplitPolicyLoop`,
      `StableSegmentCoordinator`, and a smaller set of explicit maintenance
      seams.

[x] 80.37 Rework operation package count by collapsing trivial result/mapper glue where possible (Risk: LOW)
    - Removed iterator-opening and stable-gateway glue seams so the operation
      package keeps behavior close to the actual routed read/write boundaries.

[x] 80.38 Revisit public vs package-private visibility across `core.*` (Risk: LOW)
    - Shrunk visibility to stable seams only and hid concrete maintenance,
      operation, split, executor, and observability implementation types where
      callers no longer need them.

[x] 80.39 Run a final naming sweep once structural seams stop moving (Risk: LOW)
    - Closed after the final rename-only pass across `core.*` and the last
      local vocabulary cleanup in runtime/lifecycle naming.

[x] 80.39.a Run final rename-only pass across `core.*`
    - Completed without structural moves.

[x] 80.39.b Run final method/field naming pass
    - Normalized local vocabulary such as `create(...)`, `storage()`,
      `splits()`, `services()`, and the last remaining factory helper method
      names.

[x] 80.40 Finish when the dominant remaining issues are mostly names, comments, and local method shape (Risk: LOW)
    - Closed after the final audit showed that the remaining adapters and
      decorators are behavior-owning lifecycle/infrastructure types, not
      empty pass-through hot-path shells.

[x] 80.40.a Verify no obvious wrapper-only hot-path types remain
    - Remaining wrappers are lifecycle/infrastructure decorators with concrete
      behavior such as MDC scoping or managed close, not empty forwarding
      layers in the main runtime graph.

[x] 80.40.b Verify dominant remaining findings are local readability issues
    - The remaining cleanup surface is now mostly local naming, comments, and
      small method shape rather than another structural refactor wave.

[x] 80.26.a Decide executor topology ownership
    - Chose eager ownership only for observed hot-path maintenance pools.
    - Chose lazy ownership for split-policy scheduling and
      registry-maintenance support executors.

[x] 80.26.b Implement lazy creation where it actually reduces startup noise
    - Implemented `LazyExecutorReference` for support executors.
    - `IndexExecutorRegistry` now keeps runtime-observed pools eager while
      creating split-policy and registry-maintenance executors only on first
      access.

[x] 80.27 Replace inheritance-based policy in `IndexInternalConcurrent` with composition (Risk: MEDIUM)
    - Removed subclass-only seams, moved stream behavior into `SegmentIndexImpl`,
      and replaced direct `SegmentIndexImpl` inheritance with a composition
      wrapper for the caller-thread implementation.
    - Maintenance policy stays in explicit collaborators while test unwrapping
      continues through the wrapper `delegate` field.

[x] 80.27.b Replace `IndexInternalConcurrent` inheritance with a composition wrapper
    - Replaced direct `SegmentIndexImpl` inheritance with a composition wrapper
      that delegates to a started internal `SegmentIndexImpl`.
    - Test unwrapping now goes through the wrapper `delegate` field, so the
      public/internal API shape stays intact without subclassing as the policy
      seam.

[x] 80.12 Reduce `SegmentIndexFactory` overload matrix to a smaller set of real use cases (Risk: MEDIUM)
    - `SegmentIndexFactory` now exposes only the canonical lifecycle
      entrypoints `create`, `open`, `openStored`, and `tryOpen`.
    - Convenience overloads now live only on the public `SegmentIndex` API,
      while stored/open-maybe flows route through the same canonical factory
      entrypoints instead of duplicating parallel shapes inside `core.lifecycle`.

[x] 80.12.a Remove redundant `SegmentIndexFactory` convenience overloads
    - `SegmentIndexFactory` keeps only real lifecycle entrypoints with an
      explicit chunk-filter provider registry.

[x] 80.12.b Route remaining factory entrypoints through one canonical path
    - Stored/open-maybe flows now delegate through the same canonical
      `SegmentIndexFactory` entrypoints instead of duplicating alternate
      lifecycle shapes.

[x] 80.38.a Make internal assembly/maintenance collaborators package-private
    - Introduced narrow maintenance capability seams
      `BackgroundSplitPolicyAccess` and `StableSegmentMaintenanceAccess`.
    - `BackgroundSplitPolicyLoop`, `StableSegmentCoordinator`, and the already
      hidden `IndexMaintenanceCoordinator` now stay package-private inside
      `core.maintenance`, while runtime/factory code depends only on the
      smaller maintenance-owned views.

[x] 80.38.b Make operation/runtime helper types package-private
    - Introduced `StableSegmentAccess` and `DirectSegmentAccess`.
    - `StableSegmentGateway` and `DirectSegmentCoordinator` are now
      package-private, while runtime and maintenance keep only the smaller
      routed-operation capability seams.

[x] 80.34.a Reduce runtime graph assembly to `storage -> split -> services`
    - `SegmentIndexRuntimeGraphBuilder` now reads directly as
      `core storage -> split state -> service state`.
    - The runtime path no longer bounces through extra split/runtime bags just
      to reach the final assembled runtime.

[x] 80.34.b Remove leftover assembly helper types that no longer buy readability
    - Removed `SegmentIndexLifecycleAssembly` and
      `SegmentIndexRuntimeAssembly`.
    - `SegmentIndexCoreGraph` now assembles runtime, consistency,
      facades, close, and startup collaborators directly through named helper
      methods instead of routing through additional root-level assembly bags.

[x] 80.33.c Collapse prepared split/materialization wrapper chain
    - Removed `PreparedRouteSplit`, `PreparedSegmentHandle`,
      `DefaultPreparedSegmentHandle`, and `SegmentMaterializationFileSystem`.
    - Split child preparation and materialization now live directly in the
      owning split services without a one-use wrapper stack.

[x] 80.33.d Reduce `RouteSplitCoordinator` ownership
    - Split preparation moved into `RouteSplitPreparationService` and
      publish/cleanup moved into `RouteSplitPublishCoordinator`.
    - `RouteSplitCoordinator` now stays focused on eligibility,
      current-segment validation, and handing the parent segment into the
      preparation flow instead of owning the full route split lifecycle.

[x] 80.31.a Move remaining observability-owned classes out of `core.runtime`
    - Runtime no longer owns observability-only helper classes or pass-through
      metrics bags.
    - Observability-owned snapshot and logging concerns now live under
      `core.observability`, `core.lifecycle`, or `core.infrastructure`
      according to ownership.

[x] 80.31.b Move remaining maintenance-owned classes out of `runtime/control`
    - Runtime/control no longer own maintenance-loop or stable-segment
      implementation classes.
    - Maintenance flow now lives behind
      `BackgroundSplitPolicyAccess` / `StableSegmentMaintenanceAccess`
      inside `core.maintenance`.

[x] 80.31.c Re-pack visible seams by ownership
    - Visible seams now live under the packages that actually own them:
      split under `core.split`, maintenance under `core.maintenance`,
      lifecycle-managed MDC under `core.lifecycle`, and stable executor
      snapshot interfaces under `core.observability` with package-private
      infrastructure implementations.
    - The remaining issues are now mostly naming and final readability
      cleanup, not package-placement drift from earlier refactor waves.

[x] 80.33.b Collapse trivial stable-segment direct read/write glue
    - Merged `DirectSegmentReadCoordinator` and
      `DirectSegmentWriteCoordinator` into `DirectSegmentCoordinator`.
    - Runtime split state and operation wiring now depend on one direct
      routed-access boundary instead of parallel read/write glue.

[x] 80.33.a Collapse trivial stable-segment topology/read helpers into one boundary
    - Folded `StableSegmentTopologyLookup`, `StableSegmentReadTarget`, and the
      remaining `StableSegmentGateway` load-target/helper seams into direct
      stable gateway logic.
    - Stable reads now flow from route snapshot to loaded segment access
      without extra topology/read-target wrapper types.

[x] 80.36.b Merge remaining stable-segment maintenance one-use helpers
    - Folded `BackgroundSplitCandidateScheduler` into
      `BackgroundSplitPolicyLoop`.
    - Candidate scheduling now lives directly in the loop/work-state boundary,
      so the maintenance package is down to the real long-lived owners.

[x] 80.35.b Merge observability snapshot/input glue
    - Removed `SegmentIndexMetricsSnapshotInputsCollector` and
      `SegmentIndexMetricsRequestCounter`.
    - `SegmentIndexMetricsCollector` now collects runtime inputs and resolves
      maintenance request high-water marks directly.

[x] 80.27.a Remove remaining subclass-only seams from `IndexInternalConcurrent`
    - Removed all remaining test-only `extends IndexInternalConcurrent`
      seams by switching tests to direct instantiation or targeted
      `SegmentIndexImpl` subclasses.
    - Marked `IndexInternalConcurrent` as `final`.

[x] 80.37.a Collapse trivial stable-segment gateway helper seams
    - Folded `StableSegmentHandleAccess`, `StableSegmentResultMapper`,
      `StableSegmentTopologyLookup`, and `StableSegmentReadTarget` into
      `StableSegmentGateway`.

[x] 80.37.b Collapse trivial operation retry/value-guard glue
    - Folded `IndexOperationRetryRunner` and `IndexOperationValueGuard` into
      `IndexOperationCoordinator`.

[x] 80.31.d Move split assembly ownership fully into `core.split`
    - Deleted `BackgroundSplitCoordinatorFactory`.
    - Moved split assembly onto `BackgroundSplitCoordinator.create(...)`.
    - Runtime now depends on the split runtime contract instead of a separate
      split assembly helper type.

[x] 80.38.c Shrink `core.split` public surface to stable contracts only
    - Moved `BackgroundSplitMetrics` under `core.observability`.
    - `core.split` public surface is now down to `BackgroundSplitCoordinator`
      and `RouteSplitPlan`.

[x] 80.35.a Merge low-value observability counter/set microtypes
    - Collapsed thin `Stats*Set` types into `Stats` / `StatsLatencySet`.
    - Folded `SegmentIndexMetricsSnapshotInputs` into
      `SegmentIndexMetricsSnapshotFactory`.
    - The remaining observability helper types now carry runtime behavior or
      a real metrics aggregate.

[x] 80.35.c Hide observability implementation details behind smaller public seams
    - Removed the public `SegmentIndexMetricsAccess` seam and now route
      runtime/control through `Supplier<SegmentIndexMetricsSnapshot>` from
      `SegmentIndexMetricsSnapshots.create(...)`.
    - Concrete collector and executor snapshot implementation types now stay
      hidden behind stable metrics snapshot suppliers and executor snapshot
      interfaces.

[x] 80.36.a Merge remaining background split scheduling helpers
    - Folded `BackgroundSplitPolicyGate` and
      `BackgroundSplitPolicyAwaiter` into `BackgroundSplitPolicyLoop`.
    - Earlier cleanup had already removed request/dispatcher/autonomous-loop
      helper seams, so the scheduling path now reads directly off the loop,
      work state, and candidate scheduler.

[x] 80.32 Remove test-only production hooks that exist only for white-box assertions (Risk: MEDIUM)
    - Replaced those white-box assertions with test-only access helpers and
      collaborator-focused tests.
    - Removed `IndexInternalConcurrent.testStateCoordinator()`.
    - Removed `IndexInternalConcurrent.transitionToErrorForTest()`.
