# Range-Partitioned Ingest Implementation Notes

This file keeps its historical name, but the ingest-overlay runtime it
described has been removed. The current implementation contract is:

## Current Implementation Scope

- `put()` / `delete()` append to WAL first when WAL is enabled
- writes are routed directly to stable segments through
  `DirectSegmentCoordinator`
- `get()` reads through `DirectSegmentCoordinator`
- `KeyToSegmentMap` remains the persisted routing source of truth
- `SegmentTopology` owns runtime route availability and split drain state
- WAL replay restores writes by reusing the same direct write path on open

## Split Behavior

- split is no longer an overlay reassignment workflow
- `SplitPolicyCoordinator` schedules route-first split work in the
  background
- `RouteSplitCoordinator` materializes child stable segments from the parent
  stable snapshot before route publish
- `SegmentTopology` drains the parent route before child materialization, so
  no new writes can enter the old parent while the split snapshot is being
  materialized
- writes to the affected route may be retried internally as `BUSY` while the
  route is draining or while the topology catches up to a newer route-map
  snapshot

## Read and Write Semantics

- `put()` and `delete()` append to WAL, then write directly to the routed
  segment
- `get()` resolves the current route and reads directly from the routed
  segment
- read-after-write is guaranteed by the segment write cache
- `FULL_ISOLATION` iteration opens against a route snapshot and retries if the
  route map changes while the iterator is being opened
- `flushAndWait()` and `compactAndWait()` wait for scheduled split work,
  establish the final mapped-segment boundary, flush routing metadata, and then
  checkpoint WAL

## Recovery Contract

- there is no unpublished ingest-overlay state anymore
- startup reconstructs routing from persisted index metadata
- WAL replay restores acknowledged writes through the direct routed write path
- startup and explicit consistency checks delete orphaned segment directories
  that are not referenced from the persisted routing metadata
- durability after `flushAndWait()` is defined by successful mapped-segment
  flush plus WAL checkpoint

## Configuration Migration

Legacy partition-named persisted properties and metrics still exist for
compatibility, while Java configuration uses grouped section names:

- `maxNumberOfKeysInActivePartition` -> `writePath().segmentWriteCacheKeyLimit()`
- `maxNumberOfImmutableRunsPerPartition` -> `writePath(...).legacyImmutableRunLimit()`
- `maxNumberOfKeysInPartitionBuffer` -> `writePath().maintenanceWriteCacheKeyLimit()`
- `maxNumberOfKeysInIndexBuffer` -> `writePath().indexBufferedWriteKeyLimit()`
- `maxNumberOfKeysInPartitionBeforeSplit` -> `writePath().segmentSplitKeyThreshold()`

They now act as compatibility names for routed write and split limits rather
than for a dedicated ingest-overlay runtime.
