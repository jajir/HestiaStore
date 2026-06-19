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
| Session and lifecycle | `SegmentIndexImpl`, `SegmentIndexTrackedOperationRunner`, `SegmentIndexOperationGate`, `SegmentIndexStateMachine`, `IndexCloseCoordinator` | API method implementation, lifecycle state checks, lifecycle/lock ownership, close safety, context logging, and operation tracking for one live index session. `SegmentIndexSessionFactory` is the session composition point. |
| Operation access | `IndexOperationCoordinator`, `SegmentIndexMaintenanceSessionAdapter`, `MaintenanceService` | Small call-specific boundaries for point operations, iterator operations, and foreground maintenance. |
| Runtime opening | `SegmentIndexBootstrapOperation`, `SegmentIndexSessionFactory`, `IndexCloseCoordinator` | Long-lived runtime graph for one open index: core storage, topology, WAL, metrics, runtime tuning, and service wiring. The bootstrap operation opens runtime resources in order and owns rollback cleanup until the session close coordinator takes ownership. |
| Topology runtime | `SegmentTopologyRuntimeAccess`, `SegmentTopology`, `SplitService` | Segment route topology, split runtime, iterator invalidation, direct segment access, and recovery cleanup. Topology is created during bootstrap because it depends on storage, executors, runtime state, and failure handling. |
| Core storage | `StorageService`, `StorageServiceBuilder`, `CoreStorageRuntime`, `IndexWalCoordinatorDelegate` | Provides the storage package access point, assembles storage runtime services from already opened route map and segment registry, and keeps storage-only helpers such as WAL coordination behind the service boundary. Physical key map, chunk cache, and registry opening stay explicit in the bootstrap operation so dependencies stay visible. |
| Point operations | `IndexOperationCoordinator` | Point `put`, `get`, `delete`, WAL append/replay, applied LSN recording, request counters, and operation latency metrics. |
| Route and segment lease access | `SegmentLeaseService`, `SegmentLease`, `SegmentSplitLease`, `KeyToSegmentMap`, `SegmentTopology` | Key-to-segment lookup, route snapshot validation, route leases/drains, registry-backed segment loading, retry on stale/draining routes, and scoped access to routed or split-drained segments. |
| Stable segment operations | `StableSegmentOperationGateway`, `SegmentStreamingService`, `MaintenanceService` | Single-attempt stable-segment calls used by iterator and maintenance paths, with `OK`, `BUSY`, `CLOSED`, and `ERROR` translated into index-level retry decisions. |
| Registry and segment handle | `SegmentRegistry`, `BlockingSegment` | Segment cache, lifecycle, loading/reloading, materialization helpers, runtime tuning view, and retry-aware access to a loaded segment. `BlockingSegment` is the current segment handler/handle layer. |
| Segment engine | `Segment`, `SegmentImpl`, `SegmentCore`, `SegmentSearcher`, `SegmentCompacter` | Segment-local reads, writes, flush, compaction, consistency checking, caches, sparse index, Bloom filter, and on-disk files. |

## Where to Look

- Public API behavior: start at `SegmentIndex` and `SegmentIndexImpl`.
- Create/open flow: inspect `SegmentIndexBootstrapOperation` and
  `SegmentIndexSessionFactory`.
- Operation rejected during close/open/error: inspect
  `SegmentIndexTrackedOperationRunner`, `SegmentIndexOperationGate`, and
  `SegmentIndexStateMachine`.
- Point `put`, `get`, or `delete`: inspect `IndexOperationCoordinator`.
- Key routing, route drains, split leases, or stale topology retries: inspect
  `SegmentLeaseService`, `KeyToSegmentMap`, and `SegmentTopology`.
- Segment loading, registry cache, or retry-aware segment handles: inspect
  `SegmentRegistry` and `BlockingSegment`.
- Iterator and stream behavior: inspect `SegmentTopologyRuntimeAccess`,
  `SegmentStreamingService`, and `DirectSegmentCoordinator`.
- Flush, compaction, and wait semantics: inspect `MaintenanceService`,
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
