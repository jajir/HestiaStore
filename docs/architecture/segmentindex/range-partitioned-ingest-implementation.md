# Range-Partitioned Ingest Implementation Notes

This page records the implementation contract for the partitioned ingest
runtime introduced above stable segment storage.

## Current Implementation Scope

- user writes enter an in-memory partition overlay first
- stable segments remain the durable publish target
- `KeyToSegmentMap` remains the persisted routing source of truth
- WAL replay restores unpublished writes back into the partition overlay on
  open

The current implementation changes the user write path first. It does not yet
replace every historical split helper in one step.

## Read and Write Semantics

- `put()` and `delete()` append to WAL, then update the routed partition
  overlay
- `get()` reads overlay first and stable segment storage second
- a successful `put()` is therefore visible to `get()` before any drain
  completes
- `flushAndWait()` seals active partition data, drains immutable runs into
  stable segment storage, flushes stable segments, and checkpoints WAL

## Drain Contract

- active mutable partition data rotates into immutable runs
- immutable runs remain readable until they are successfully drained and
  flushed into stable storage
- drain work is scheduled on index maintenance executors
- if the overlay exceeds local or global limits, writes receive bounded
  backpressure instead of waiting on a live segment split

## Recovery Contract

- unpublished partition overlay state is transient
- startup reconstructs routing from persisted index metadata
- WAL replay restores acknowledged writes that were not yet published
- durability after `flushAndWait()` is defined by successful stable-segment
  flush plus WAL checkpoint

## Configuration Migration

New partition-oriented settings:

- `maxNumberOfKeysInActivePartition`
- `maxNumberOfImmutableRunsPerPartition`
- `maxNumberOfKeysInPartitionBuffer`
- `maxNumberOfKeysInIndexBuffer`
- `maxNumberOfKeysInPartitionBeforeSplit`

Legacy persisted settings are still accepted during load and are migrated to
the new keys:

- `maxNumberOfKeysInSegmentWriteCache` ->
  `maxNumberOfKeysInActivePartition`
- `maxNumberOfKeysInSegmentWriteCacheDuringMaintenance` ->
  `maxNumberOfKeysInPartitionBuffer`
- `maxNumberOfKeysInSegment` ->
  `maxNumberOfKeysInPartitionBeforeSplit`

New manifests are written with the partition-oriented names only.
