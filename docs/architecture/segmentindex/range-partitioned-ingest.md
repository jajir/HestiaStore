---
title: Range-Partitioned Ingest Compatibility Note
audience: contributor
doc_type: explanation
owner: engine
---

# Range-Partitioned Ingest Compatibility Note

This file keeps its historical topic name, but the ingest-overlay runtime it
described has been removed. `SegmentIndex` now routes writes directly into
stable segments and relies on segment-local write caches for read-after-write
semantics.

Use this page as a compatibility note explaining what replaced the removed
historical ingest runtime.

## Goals

- `put()` becomes visible to `get()` immediately after the call returns.
- Stable segment files remain the durable storage boundary.
- Split work stays route-first and atomic at the map-publish step.
- Overload is expressed through bounded retry and explicit `BUSY`, not through
  an extra overlay runtime.

## Current Runtime Model

- User writes target the routed stable segment directly.
- `KeyToSegmentMap` remains the persisted routing source of truth.
- WAL remains the crash-recovery mechanism for acknowledged writes that are not
  yet flushed to stable segment files.
- Read-after-write comes from the segment write cache, not from an index-level
  overlay.

## Read Contract

`get(key)` resolves the current route and then reads the mapped segment. The
segment checks its write cache and delta cache before consulting stable
on-disk structures.

## Write Contract

`put()` / `delete()` append to WAL first when enabled, then resolve the write
route and call `Segment.put(...)` on the mapped segment. The write is visible
immediately through the segment write cache and becomes durable after segment
flush plus WAL checkpoint.

## Split Model

Split has two phases:

1. background materialization builds child stable segments from the parent
   stable snapshot
2. a short publish step atomically remaps `KeyToSegmentMap`

During split build, writes to the affected route may be retried internally as
`BUSY`. There is no overlay reassignment step anymore.

## Backpressure Model

Bounded retry remains mandatory:

- split-affected routes can return internal `BUSY` until the retry policy
  succeeds or times out
- WAL retention pressure can force checkpoint progress before accepting more
  writes
- segment-local maintenance still bounds how much mutable state one segment can
  hold before maintenance catches up

## Maintenance Boundaries

`flush()` schedules stable flush work but does not wait for the whole topology
to settle.

`flushAndWait()` and `compactAndWait()` are the explicit durability
boundaries:

- scheduled split work is allowed to finish and publish if it changes the
  routed topology
- stable segments for the final mapped topology are flushed
- persisted routing metadata is flushed
- WAL is checkpointed after the stable view is durable

## Recovery Model

Crash recovery rebuilds routing from persisted metadata, replays WAL through
the current direct write path, and removes orphaned split artifacts that were
never published into the route map.

## Related Docs

- [Range-Partitioned Ingest Implementation Notes](range-partitioned-ingest-implementation.md)
- [Write Path](write-path.md)
- [Read Path](read-path.md)
