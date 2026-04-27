---
title: WAL Runtime
audience: contributor
doc_type: explanation
owner: engine
---

# WAL Runtime

This page explains the current internal WAL runtime ownership in
`org.hestiastore.index.segmentindex.wal`.

`WalRuntime` remains the stable compatibility-facing entry point, but it is no
longer the implementation home for every WAL concern.

## Ownership boundaries

- `WalRuntime`: public facade for `open()`, `recover()`, `appendPut()`,
  `appendDelete()`, `onCheckpoint()`, `statsSnapshot()`, and `close()`
- `WalMetadataCatalog`: owns `format.meta`, `checkpoint.meta`, temp-file
  promotion, metadata validation, and WAL segment discovery
- `WalSegmentCatalog`: owns active segment rotation, retained-byte accounting,
  checkpoint cleanup, and runtime segment inventory
- `WalRecordCodec`: owns WAL record encoding, CRC handling, and record-body
  decoding
- `WalRecoveryManager`: owns replay scan flow, invalid-tail detection,
  corruption-policy handling, and checkpoint clamp behavior during recovery
- `WalWriter`: owns append-path orchestration above storage append, segment
  admission, metrics, and durability-policy delegation
- `WalSyncPolicy`: owns `ASYNC` vs `SYNC` vs `GROUP_SYNC` durability behavior,
  pending-sync batching, durable LSN tracking, and sync-failure state
- `WalRuntimeMetrics`: owns WAL append/sync/corruption counters and snapshot
  assembly

## Concurrency model

- `WalRuntime` owns one monitor that guards stateful WAL operations.
- Recovery, append, checkpoint, cleanup, and close run while holding that
  monitor.
- `WalSyncPolicy` may run a scheduled group-sync task, but it reacquires the
  same monitor before mutating durability state.
- `durableLsn()` remains available as a best-effort read without requiring
  callers to hold the monitor.

## Metadata and catalog model

The current extraction step introduces one internal catalog view for WAL:

- format metadata from `wal/format.meta`
- checkpoint metadata from `wal/checkpoint.meta`
- discovered WAL segment inventory from `wal/*.wal`

This is intentionally still WAL-local. It is the bridge toward the broader
index-state catalog work tracked in backlog item `84`.

## Stable invariants

- Segment file naming remains `<20-digit-base-lsn>.wal`.
- Checkpoint LSN remains monotonic at runtime.
- Recovery still validates monotonic LSN ordering across segment boundaries.
- `TRUNCATE_INVALID_TAIL` vs `FAIL_FAST` semantics remain unchanged.
- Public `WalRuntime` metrics and log event names remain unchanged.

## Related docs

- [WAL operations guide](../../operations/wal.md)
- [Consistency & Recovery](../recovery.md)
- [WAL Replication & Fencing Design](../../development/wal-replication-fencing-design.md)
