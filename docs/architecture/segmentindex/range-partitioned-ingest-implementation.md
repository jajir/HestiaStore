# Range-Partitioned Ingest Implementation Notes

This file keeps its historical name, but the ingest-overlay runtime it
described has been removed. The current implementation contract is:

## Current Implementation Scope

- `put()` / `delete()` append to WAL first when WAL is enabled
- writes are routed directly to stable segments through
  `DirectSegmentWriteCoordinator`
- `get()` reads through `DirectSegmentReadCoordinator`
- `KeyToSegmentMap` remains the persisted routing source of truth
- WAL replay restores writes by reusing the same direct write path on open

## Split Behavior

- split is no longer an overlay reassignment workflow
- `BackgroundSplitCoordinator` schedules route-first split work in the
  background
- `RouteSplitCoordinator` materializes child stable segments from the parent
  stable snapshot before route publish
- the split admission gate protects both split scheduling and the short
  route-map publish step
- writes to the affected route may be retried internally as `BUSY` while a
  split is scheduled or publishing

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

Legacy partition-named settings still exist in configuration and metrics for
compatibility:

- `maxNumberOfKeysInActivePartition`
- `maxNumberOfImmutableRunsPerPartition`
- `maxNumberOfKeysInPartitionBuffer`
- `maxNumberOfKeysInIndexBuffer`
- `maxNumberOfKeysInPartitionBeforeSplit`

They now act as compatibility names for routed write and split limits rather
than for a dedicated ingest-overlay runtime.
