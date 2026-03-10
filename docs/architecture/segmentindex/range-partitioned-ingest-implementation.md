# Range-Partitioned Ingest Implementation Notes

This page records the implementation contract for the partitioned ingest
runtime introduced above stable segment storage.

## Current Implementation Scope

- user writes enter an in-memory partition overlay first
- stable segments remain the durable publish target
- `KeyToSegmentMap` remains the persisted routing source of truth
- WAL replay restores unpublished writes back into the partition overlay on
  open

The current implementation changes the user write path first. It still uses
the existing stable-segment split execution primitives, but the runtime no
longer depends on the historical `SegmentSplitCoordinator` wrapper.

## Transitional Split Behavior

- live-segment split is no longer triggered directly from `put()`
- the current transition slice only evaluates partition-aware stable split
  scheduling on explicit maintenance boundaries such as `flushAndWait()` and
  `compactAndWait()`
- when one of those explicit maintenance calls decides to split, the split now
  executes synchronously inside that maintenance call instead of being handed
  off to the old async split queue
- if buffered overlay data still exists for the parent route at split-apply
  time, it is reassigned to the produced child routes as part of the same
  partition-aware split apply step instead of being left on the retired parent
  segment id
- background overlay drain itself does not schedule live split work, which
  avoids reintroducing split-vs-drain contention on the hot write path
- point `get()` now runs under the same short split-apply read gate as `put()`,
  so remap plus overlay reassignment cannot expose a stale point lookup during
  the split apply window
- explicit maintenance split no longer holds that gate for the whole stable
  child build; the gate only wraps the final split-apply remap window
- while a split is building child stable segments, drain back into the parent
  route is temporarily suspended so newly buffered data stays in overlay and is
  reassigned to child routes if the split applies

## Read and Write Semantics

- `put()` and `delete()` append to WAL, then update the routed partition
  overlay
- `get()` reads overlay first and stable segment storage second
- a successful `put()` is therefore visible to `get()` before any drain
  completes
- `FULL_ISOLATION` index streaming now opens against a split-safe route
  snapshot and retries if the segment map changes underneath the open; it no
  longer falls back to the historical split-idle barrier on the read path
- point operations no longer wait explicitly for background live-segment split
  completion before retrying a `BUSY` path
- `flushAndWait()` seals active partition data, drains immutable runs into
  stable segment storage, flushes stable segments, runs eligible partition-aware
  stable splits, and checkpoints WAL
- `compactAndWait()` likewise performs its explicit maintenance work under the
  same split-safe maintenance boundary

## Drain Contract

- active mutable partition data rotates into immutable runs
- immutable runs remain readable until they are successfully drained and
  flushed into stable storage
- drain work is scheduled on index maintenance executors
- if the overlay exceeds local or global limits, writes receive bounded
  backpressure instead of waiting on a live segment split
- during the brief split-apply window itself, write admission is still gated
  so route remap and overlay reassignment stay atomic, but explicit
  `flushAndWait()` / `compactAndWait()` no longer need a whole-operation global
  write gate

## Recovery Contract

- unpublished partition overlay state is transient
- startup reconstructs routing from persisted index metadata
- WAL replay restores acknowledged writes that were not yet published
- startup and explicit consistency checks delete orphaned segment directories
  that are not referenced from the persisted routing metadata, which covers
  abandoned route-first split children after interrupted maintenance
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
