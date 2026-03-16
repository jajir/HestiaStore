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
- the current transition slice evaluates partition-aware stable split
  scheduling from a coalesced background policy scan over current routed
  partitions
- when the background split policy decides a routed partition should split,
  child stable segments are materialized from the parent stable snapshot before
  the final route remap is published
- if buffered overlay data still exists for the parent route at split-apply
  time, it is reassigned to the produced child routes as part of the same
  partition-aware split apply step instead of being left on the retired parent
  segment id
- background overlay drain only requests another policy scan; it does not run
  split work inline on the hot write path
- point `get()` now runs under the same short split-apply read gate as `put()`,
  so remap plus overlay reassignment cannot expose a stale point lookup during
  the split apply window
- explicit maintenance split no longer holds that gate for the whole stable
  child build; the gate only wraps the final split-apply remap window
- while a split is building child stable segments, drain back into the parent
  route is temporarily suspended so newly buffered data stays in overlay and is
  reassigned to child routes if the split applies
- there is no longer a runtime-only pending split fallback chain after route
  apply; by the time the new route becomes visible, the child stable data
  already exist on disk

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
  stable segment storage, waits for any partition-aware stable splits already
  scheduled by background drain, drains again if split apply reassigned overlay
  data to child routes, waits again for any second-wave split scheduled by that
  follow-up drain, flushes stable segments, and if the final idle split-policy
  scan still publishes child routes, reestablishes the flush boundary on the
  final mapped segments before checkpointing WAL; explicit maintenance
  boundaries also bypass split retry cooldown so transient split aborts do not
  escape into a later autonomous retry window
- `compactAndWait()` likewise waits for any split already scheduled by
  background drain before compacting stable segments, so compaction does not
  overlap with split materialization of the same routed range; the same second
  drain plus wait cycle is repeated before compaction begins, and if the final
  idle split-policy scan still publishes child routes, `compactAndWait()`
  reestablishes the compact/flush boundary on the final mapped segments before
  returning

## Drain Contract

- active mutable partition data rotates into immutable runs
- immutable runs remain readable until they are successfully drained and
  flushed into stable storage
- drain work is scheduled on index maintenance executors
- partition-aware stable split build work is scheduled on a dedicated
  split-maintenance executor so the heavy child materialization phase no
  longer runs on the explicit maintenance caller thread
- a periodic autonomous background split policy loop runs while the index is
  open; it keeps reevaluating routed stable ranges even when no new writes,
  reopen, or runtime-threshold patch occurs
- the autonomous loop is driven by a dedicated split-policy scheduler owned by
  the index executor registry, so `close()` shuts down both split execution and
  future policy ticks together with the rest of index maintenance infrastructure
- that autonomous loop only performs a full routed scan when overlay, drain,
  and split backlog are idle; hot write periods feed the loop with post-drain
  per-partition hints, so split candidate evaluation stays in background
  instead of inline scheduling on the drain worker
- split scheduling keeps a per-segment cooldown and retry-growth hysteresis
  window; if a borderline split candidate fails or aborts, the background loop
  does not immediately thrash on the same stable range again unless either
  enough time passes or the routed segment grows materially
- that cooldown is adaptive rather than fixed: longer split attempts stretch
  the next retry window, while short split attempts decay it back toward the
  baseline
- additional immediate background split policy scans are still triggered on
  open, after consistency repair, and after runtime split-threshold changes
- explicit `flushAndWait()` / `compactAndWait()` still keep split candidate
  evaluation on the background policy path, but before returning they also
  wait for one final idle split-policy scan triggered after explicit
  maintenance so routed segment backlog does not leak past the boundary
- while explicit stable flush/compaction is running, new autonomous split
  candidates are temporarily ignored; a fresh idle scan is requested right
  after explicit maintenance completes so split materialization does not race
  against that maintenance on the same routed range
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
  abandoned split children after interrupted maintenance
- durability after `flushAndWait()` is defined by successful stable-segment
  flush plus WAL checkpoint

## Configuration Migration

New partition-oriented settings:

- `maxNumberOfKeysInActivePartition`
- `maxNumberOfImmutableRunsPerPartition`
- `maxNumberOfKeysInPartitionBuffer`
- `maxNumberOfKeysInIndexBuffer`
- `maxNumberOfKeysInPartitionBeforeSplit`

Compatibility fallbacks during load map older or cross-version settings to the
new partition keys:

- `maxNumberOfKeysInSegmentWriteCache` ->
  `maxNumberOfKeysInActivePartition`
- `maxNumberOfKeysInSegmentWriteCacheDuringMaintenance` ->
  `maxNumberOfKeysInPartitionBuffer`
- `maxNumberOfKeysInSegment` (when
  `maxNumberOfKeysInPartitionBeforeSplit` is absent) ->
  `maxNumberOfKeysInPartitionBeforeSplit`
- `segmentMaintenanceAutoEnabled` ->
  `backgroundMaintenanceAutoEnabled`

New manifests are written with the partition-oriented names only.
