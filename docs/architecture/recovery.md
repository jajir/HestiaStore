# Consistency & Recovery

This page explains HestiaStore's crash-safety model and commit semantics. WAL
is optional and disabled by default (`Wal.EMPTY`). Without WAL, durability is
driven by explicit maintenance completion plus temp-file + atomic-rename commit
paths. With WAL enabled, writes are appended before apply and startup replays
WAL records above checkpoint, with invalid-tail truncation or fail-fast based
on policy.

## Scope and Guarantees

- WAL-disabled mode: no automatic WAL replay. Durability boundary is
  `flushAndWait()` or `close()`.
- WAL-enabled mode: startup can repair an invalid WAL tail and replay durable
  records above checkpoint.
- No multi-key ACID transactions: operations are per-key; there is no
  cross-key atomic batch commit.
- Durability boundary: `flushAndWait()` or `close()` persists all writes that
  happened before the call. `flush()` only schedules maintenance.
- Atomic file replacement: data files are written to `*.tmp` and made visible
  by `rename` only after the writer is closed and committed.

## Where Writes Become Durable

- Segment-local write cache -> disk:
  `SegmentIndex.flush()` schedules per-segment maintenance. `flushAndWait()`
  and `close()` wait until the final mapped stable segments are flushed.
- Segment compaction:
  when a segment compacts, the new main SST, sparse index, and Bloom filter
  are built via transactional writers and atomically replace the old view.
- Key-to-segment map (`index.map`):
  persisted through a transactional sorted-data writer whenever routing must be
  flushed.
- WAL checkpoint:
  after the stable segment state and route map are durable, WAL checkpoint
  advances so replay no longer needs older records.

Relevant code:
`segmentindex/core/maintenance/MaintenanceService.java`,
`segmentindex/core/maintenance/MaintenanceServiceImpl.java`,
`segmentindex/core/operations/IndexOperationCoordinator.java`,
`segmentindex/mapping/KeyToSegmentMap.java`.

## Transactional Write Primitives

All main data files follow the same pattern: write to a temporary file, then atomically rename on `commit()`.

- Guarded transactions: `GuardedWriteTransaction` requires the resource to be closed before `commit()` and prevents double‑commit.
- Single‑call helper: `WriteTransaction.execute(writer -> { … })` does open → write → close → commit.

Key classes:
- `unsorteddatafile/UnsortedDataFileWriterTx` → `rename(temp, final)` on commit
- `sorteddatafile/SortedDataFileWriterTx` → `rename(temp, final)` on commit
- `datablockfile/DataBlockWriterTx` → used by chunk store writers
- `chunkstore/ChunkStoreWriterTx` and `chunkentryfile/ChunkEntryFileWriterTx` → layered over `DataBlockWriterTx`
- `bloomfilter/BloomFilterWriterTx` → writes new filter and swaps it in on commit

## File Types and Commit Paths

- Segment delta cache files
  - Writer: `segment/SegmentDeltaCacheWriter`
  - Mechanism: `SortedDataFileWriterTx.execute(…)`
  - Naming: manifest counter assigns `vNN-delta-NNNN.cache` before write; if a crash happens before commit, the reader treats missing files as empty, so boot remains safe.

- Main SST (chunked) + sparse index ("scarce index")
  - Writers: `segment/SegmentFullWriterTx` and `segment/SegmentFullWriter`
  - Internals: `ChunkEntryFileWriterTx` for SST, `ScarceIndexWriterTx` for the sparse index
  - Bloom filter: `BloomFilterWriterTx` builds a new filter and commits (rename) before the SST and sparse index are committed. This ordering avoids false negatives on restart.

- Bloom filter
  - Writes to a temporary file via `BloomFilterWriterTx.open()` and commits with `rename`; also updates the in‑memory hash snapshot on commit.

- Key→segment map (`index.map`)
  - Writer: `SortedDataFileWriterTx.execute(…)` inside `KeyToSegmentMap.flushIfDirty()`
  - Ensures the map is replaced atomically.

## What Is Not Transactional

- Segment manifest metadata (counts and delta‑file numbering) is persisted via an overwrite (`Directory.Access.OVERWRITE`). It is updated after data files are committed, and is not critical to data correctness. If a crash corrupts or desynchronizes this metadata, the reader logic remains safe (e.g., missing delta file names yield empty reads) and you can re‑establish consistency via the checker below.

Code: `properties/PropertyStoreImpl` and `SegmentPropertiesManager`.

## Failure Model (Examples)

- Crash while writing a delta file before commit: only `*.tmp` exists; it is ignored on boot; prior state remains valid.
- Crash after committing a Bloom filter but before committing SST/sparse index: Bloom filter is ahead of data, which is safe (may increase positives but never produce false negatives).
- Crash after committing SST/sparse index but before properties update: data is fully committed; metadata may lag but does not affect correctness.

## Consistency Check and Repair

- Run `SegmentIndex.checkAndRepairConsistency()` after an unexpected shutdown to verify that segments are well‑formed and sorted and that the key→segment map is coherent. This walks all segments, checks ordering and basic invariants, and raises an error if it finds non‑recoverable issues.

Key classes: `segmentindex/IndexConsistencyChecker`, `segment/SegmentConsistencyChecker`.

## Developer Notes: `open()`/`commit()` and `*.tmp`

- `open()` returns a writer bound to a temporary file (typically with a `.tmp` suffix). You must close the writer before calling `commit()`.
- `commit()` performs an atomic `rename(temp, final)` so either the old file or the new file is visible on disk.
- Prefer `execute(writer -> {…})` to ensure the correct order: open → write → close → commit.

Examples in code:
- `sorteddatafile/SortedDataFileWriterTx#open()` → `commit()` renames temp to final
- `unsorteddatafile/UnsortedDataFileWriterTx#open()` → `commit()` renames temp to final
- `datablockfile/DataBlockWriterTx#open()` → `commit()` renames temp to final
- `bloomfilter/BloomFilterWriterTx#open()` → `commit()` renames temp to final and swaps hash

## Practical Guidance

- If WAL is disabled, call `flushAndWait()` on periodic boundaries and always before shutdown to persist in‑memory writes.
- If another thread observes the index during shutdown, expect `getState()` /
  `metricsSnapshot().getState()` to report `CLOSING` until the final `CLOSED`
  transition.
- If WAL is enabled, configure durability mode (`ASYNC`, `GROUP_SYNC`, `SYNC`) based on loss tolerance and latency targets.
- After a crash, reopen the index; WAL-enabled indexes rebuild routing, replay
  WAL through the direct write path, and then `checkAndRepairConsistency()` can
  be run as an additional integrity check.

## Related Glossary

- [Flush](glossary.md#flush)
- [Write Transaction](glossary.md#write-transaction)
- [Compaction](glossary.md#compaction)
- [Consistency Checker](glossary.md#consistency-checker)
