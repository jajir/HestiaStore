---
title: SegmentIndex Implementation
audience: contributor
doc_type: explanation
owner: engine
---

# SegmentIndex Implementation

This page is a locator map for the `SegmentIndex` implementation. It shows the
main layers from the public `SegmentIndex` interface down to the physical
`Segment` implementation, so maintainers can quickly decide where to inspect a
behavior before opening the detailed read, write, split, registry, or segment
pages.

## Layer Diagram

![SegmentIndex implementation layers](images/implementation-layers.png)

Source: [implementation-layers.plantuml](images/implementation-layers.plantuml)

## Layer Responsibilities

| Layer | Main classes | Responsibility |
| ----- | ------------ | -------------- |
| Public API | `SegmentIndex`, `IndexConfiguration`, `IndexConfigurationBuilder` | External create/open and user-facing operations. This is the compatibility boundary. |
| Session and lifecycle | `IndexInternalConcurrent`, `IndexContextLoggingAdapter`, `SegmentIndexImpl`, `SegmentIndexTrackedOperationRunner`, `IndexOperationTracker`, `core.session.state` | API method implementation, lifecycle state checks, lifecycle/lock ownership, close safety, context logging, and operation tracking for one live index session. `SegmentIndexImpl.open(...)` is the session composition point and keeps failed-startup cleanup local to session ownership. |
| Operation facades | `SegmentIndexPointOperationFacade`, `SegmentIndexReadFacade`, `MaintenanceService` | Small call-specific boundaries for point operations, iterator operations, and foreground maintenance. |
| Runtime opening | `SegmentIndexBootstrapSteps`, `SegmentIndexRuntime`, `SegmentIndexRuntimeServices` | Long-lived runtime graph for one open index: core storage, topology, WAL, metrics, runtime tuning, and service wiring. Bootstrap steps open runtime resources in order and own rollback cleanup until `SegmentIndexRuntime` takes ownership. |
| Topology runtime | `SegmentTopologyRuntime`, `SegmentTopology`, `SplitService` | Segment route topology, split runtime, iterator invalidation, direct segment access, and recovery cleanup. Topology is created during bootstrap because it depends on storage, executors, runtime state, and failure handling. |
| Core storage | `SegmentIndexCoreStorage`, `SegmentIndexCoreStorageFactory`, `SegmentIndexCoreStorageOpenSpec`, `IndexWalCoordinator` | Opens and owns the storage-owned route map, segment registry, runtime tuning state, retry policy, and WAL coordination helpers without depending on session classes. |
| Point operations | `IndexOperationCoordinator`, `SegmentIndexOperationAccess` | Point `put`, `get`, `delete`, WAL append/replay, applied LSN recording, request counters, and operation latency metrics. |
| Route and segment lease access | `SegmentLeaseService`, `SegmentLease`, `SegmentSplitLease`, `KeyToSegmentMap`, `SegmentTopology` | Key-to-segment lookup, route snapshot validation, route leases/drains, registry-backed segment loading, retry on stale/draining routes, and scoped access to routed or split-drained segments. |
| Stable segment operations | `StableSegmentOperationGateway`, `StableSegmentOperationResult`, `StableSegmentOperationStatus`, `SegmentStreamingService`, `MaintenanceServiceImpl` | Single-attempt stable-segment calls used by iterator and maintenance paths, with `OK`, `BUSY`, `CLOSED`, and `ERROR` translated into index-level retry decisions. |
| Registry and segment handle | `SegmentRegistry`, `BlockingSegment` | Segment cache, lifecycle, loading/reloading, materialization helpers, runtime tuning view, and retry-aware access to a loaded segment. `BlockingSegment` is the current segment handler/handle layer. |
| Segment engine | `Segment`, `SegmentImpl`, `SegmentCore`, `SegmentSearcher`, `SegmentCompacter` | Segment-local reads, writes, flush, compaction, consistency checking, caches, sparse index, Bloom filter, and on-disk files. |

## Where to Look

- Public API behavior: start at `SegmentIndex` and `SegmentIndexImpl`.
- Create/open flow: inspect `SegmentIndexBootstrapOperation`,
  `SegmentIndexImpl.open(...)`, and `SegmentIndexBootstrapSteps`.
- Operation rejected during close/open/error: inspect
  `SegmentIndexTrackedOperationRunner`, `IndexOperationTracker`, and
  `IndexStateCoordinator` in `core.session.state`.
- Point `put`, `get`, or `delete`: inspect `SegmentIndexPointOperationFacade`,
  `SegmentIndexRuntime`, and `IndexOperationCoordinator`.
- Key routing, route drains, split leases, or stale topology retries: inspect
  `SegmentLeaseService`, `KeyToSegmentMap`, and `SegmentTopology`.
- Segment loading, registry cache, or retry-aware segment handles: inspect
  `SegmentRegistry` and `BlockingSegment`.
- Iterator and stream behavior: inspect `SegmentIndexReadFacade`,
  `SegmentTopologyRuntime` in `core.topology`, `SegmentStreamingService`, and
  `DirectSegmentCoordinator`.
- Flush, compaction, and wait semantics: inspect `MaintenanceServiceImpl`,
  `StableSegmentOperationGateway`, and `SegmentImpl`.
- Actual per-segment data layout and lookup mechanics: inspect `Segment`,
  `SegmentImpl`, `SegmentCore`, `SegmentSearcher`, and `SegmentCompacter`.
- Split scheduling and route publication: inspect `SplitService`,
  `SplitPolicyCoordinator`, `SplitExecutionCoordinator`,
  `RouteSplitCoordinator`, and `RouteSplitPublishCoordinator`.

## Related Docs

- [Read Path](read-path.md)
- [Write Path](write-path.md)
- [Segment Index Concurrency](segment-index-concurrency.md)
- [Segment Architecture](../segment/index.md)
- [Segment Registry](../registry/registry.md)
