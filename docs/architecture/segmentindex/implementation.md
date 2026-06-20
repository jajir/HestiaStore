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
| Session and lifecycle | `SegmentIndexSession`, `SessionOperationGate`, `SegmentIndexStateMachine`, `SegmentIndexRuntimeResources`, `SessionCloseCoordinator` | API method implementation, lifecycle state checks, lifecycle/lock ownership, close safety, context logging, and operation tracking for one live index session. `SegmentIndexSessionAssembler` is the session composition point. |
| Operation access | `PointOperationCoordinator`, `MaintenanceApiAdapter`, `MappedSegmentMaintenanceService` | Small call-specific boundaries for point operations, iterator operations, and foreground maintenance. |
| Runtime opening | `SegmentIndexBootstrapOperation`, `SegmentIndexSessionAssembler` | Long-lived runtime graph for one open index: core storage, topology, WAL, metrics, runtime tuning, and service wiring. The bootstrap operation opens runtime resources in order and owns rollback cleanup until the session close coordinator takes ownership. |
| Topology runtime | `RouteTopology`, `SplitRuntime`, `SegmentIteratorService` | Segment route topology, split runtime, iterator invalidation, and recovery cleanup. Topology is created during bootstrap because it depends on storage, executors, runtime state, and failure handling. |
| Split runtime | `SplitRuntime`, `SplitTaskCoordinator`, `SplitPolicyScheduler`, `RouteSplitPlanner`, `RouteSplitPublisher` | Schedules split candidates, drains mapped routes, materializes child stable segments, publishes route-map changes, and records split metrics. |
| Core storage | `StorageCoordinator`, `StorageCoordinator.create(...)`, `OpenedStorageRuntime`, `WalCoordinator`, `ActiveWalCoordinator`, `NoopWalCoordinator` | Provides the storage package access point, owns core storage close ordering, and keeps storage-only helpers such as WAL coordination behind the service boundary. Physical key map, chunk cache, and registry opening stay explicit in the bootstrap operation so dependencies stay visible. |
| Point operations | `PointOperationCoordinator` | Point `put`, `get`, `delete`, WAL append/replay, applied LSN recording, request counters, and operation latency metrics. |
| Route and segment lease access | `MappedSegmentLeaseService`, `MappedSegmentLease`, `RouteSplitLease`, `SegmentRouteMap`, `RouteTopology` | Key-to-segment lookup, route snapshot validation, route leases/drains, registry-backed segment loading, retry on stale/draining routes, and scoped access to routed or split-drained segments. |
| Stable segment operations | `NonBlockingSegmentOperationGateway`, `SegmentIteratorService`, `StableSegmentsIterator`, `MappedSegmentMaintenanceService` | Single-attempt stable-segment calls used by iterator and maintenance paths, with `OK`, `BUSY`, `CLOSED`, and `ERROR` translated into index-level retry decisions. Iterator paths obtain segment handles through `MappedSegmentLeaseService`; route leases are released after the segment iterator is opened. |
| Registry and segment handle | `SegmentRegistry`, `BlockingSegment` | Segment cache, lifecycle, loading/reloading, materialization helpers, runtime tuning view, and retry-aware access to a loaded segment. `BlockingSegment` is the current segment handler/handle layer. |
| Segment engine | `Segment`, `SegmentImpl`, `SegmentCore`, `SegmentSearcher`, `SegmentCompacter` | Segment-local reads, writes, flush, compaction, consistency checking, caches, sparse index, Bloom filter, and on-disk files. |

## Where to Look

- Public API behavior: start at `SegmentIndex` and `SegmentIndexSession`.
- Create/open flow: inspect the static factories on `SegmentIndex`,
  `SegmentIndexBootstrapOperation`, and `SegmentIndexSessionAssembler`.
- Operation rejected during close/open/error: inspect
  `SegmentIndexSession`, `SessionOperationGate`,
  `SegmentIndexStateMachine`, and `SessionCloseCoordinator`.
- Point `put`, `get`, or `delete`: inspect `PointOperationCoordinator`.
- Key routing, route drains, split leases, or stale topology retries: inspect
  `MappedSegmentLeaseService`, `SegmentRouteMap`, and `RouteTopology`.
- Segment loading, registry cache, or retry-aware segment handles: inspect
  `SegmentRegistry` and `BlockingSegment`.
- Iterator and stream behavior: inspect `SegmentIteratorService` and
  `StableSegmentsIterator`.
- Flush, compaction, and wait semantics: inspect `MappedSegmentMaintenanceService`,
  `NonBlockingSegmentOperationGateway`, and `SegmentImpl`.
- Actual per-segment data layout and lookup mechanics: inspect `Segment`,
  `SegmentImpl`, `SegmentCore`, `SegmentSearcher`, and `SegmentCompacter`.
- Split scheduling and route publication: inspect `SplitRuntime`,
  `SplitPolicyScheduler`, `SplitTaskCoordinator`,
  `RouteSplitPlanner`, and `RouteSplitPublisher`.

## Related Docs

- [Read Path](read-path.md)
- [Write Path](write-path.md)
- [Segment Index Concurrency](segment-index-concurrency.md)
- [Segment Architecture](../segment/index.md)
- [Segment Registry](../registry/registry.md)
