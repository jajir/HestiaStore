# 🛟 Consistency & Recovery

This page explains HestiaStore’s crash safety model and commit semantics. There is no WAL‑based crash recovery or multi‑operation transactions. Durability is driven by explicit flushes and by the fact that all data files are written via temporary files and atomically renamed on commit.

## 📜 Scope and Guarantees

- No automatic recovery: the system does not replay a WAL or roll back partial groups of operations after a crash.
- Durability boundary: calling `flushAndWait()` (or closing the index) persists all writes that happened before the call. `flush()` only schedules maintenance; wait for completion if you need a durability guarantee.
- Atomic file replacement: data files are written to `*.tmp` and made visible via `rename` only after the writer is closed and the transaction is committed. A crash cannot produce partially written visible files.

## 💾 Where Writes Become Durable

- Index‑level buffer → disk: `SegmentIndex.flush()` schedules draining of the in‑memory unique buffer into segment delta cache files. `flushAndWait()` (and close) wait for completion.
- Segment merge/compaction: when a segment compacts, the new main SST, sparse index, and Bloom filter are built via transactional writers; on commit they atomically replace the old ones.
- Key→segment map (`index.map`): persisted via a transactional sorted data writer during flush or when updated.

Relevant code: `segmentindex/SegmentIndexImpl#flush()`, `segmentindex/CompactSupport`, `segmentindex/KeyToSegmentMap#optionalyFlush()`.

## ✍️ Transactional Write Primitives

All main data files follow the same pattern: write to a temporary file, then atomically rename on `commit()`.

- Guarded transactions: `GuardedWriteTransaction` requires the resource to be closed before `commit()` and prevents double‑commit.
- Single‑call helper: `WriteTransaction.execute(writer -> { … })` does open → write → close → commit.

Key classes:
- `unsorteddatafile/UnsortedDataFileWriterTx` → `rename(temp, final)` on commit
- `sorteddatafile/SortedDataFileWriterTx` → `rename(temp, final)` on commit
- `datablockfile/DataBlockWriterTx` → used by chunk store writers
- `chunkstore/ChunkStoreWriterTx` and `chunkentryfile/ChunkEntryFileWriterTx` → layered over `DataBlockWriterTx`
- `bloomfilter/BloomFilterWriterTx` → writes new filter and swaps it in on commit

## 🗂️ File Types and Commit Paths

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
  - Writer: `SortedDataFileWriterTx.execute(…)` inside `KeyToSegmentMap.optionalyFlush()`
  - Ensures the map is replaced atomically.

## 🚫 What Is Not Transactional

- Segment manifest metadata (counts and delta‑file numbering) is persisted via an overwrite (`Directory.Access.OVERWRITE`). It is updated after data files are committed, and is not critical to data correctness. If a crash corrupts or desynchronizes this metadata, the reader logic remains safe (e.g., missing delta file names yield empty reads) and you can re‑establish consistency via the checker below.

Code: `properties/PropertyStoreimpl` and `SegmentPropertiesManager`.

## 💥 Failure Model (Examples)

- Crash while writing a delta file before commit: only `*.tmp` exists; it is ignored on boot; prior state remains valid.
- Crash after committing a Bloom filter but before committing SST/sparse index: Bloom filter is ahead of data, which is safe (may increase positives but never produce false negatives).
- Crash after committing SST/sparse index but before properties update: data is fully committed; metadata may lag but does not affect correctness.

## 🧰 Consistency Check and Repair

- Run `SegmentIndex.checkAndRepairConsistency()` after an unexpected shutdown to verify that segments are well‑formed and sorted and that the key→segment map is coherent. This walks all segments, checks ordering and basic invariants, and raises an error if it finds non‑recoverable issues.

Key classes: `segmentindex/IndexConsistencyChecker`, `segment/SegmentConsistencyChecker`.

## 👩‍💻 Developer Notes: `open()`/`commit()` and `*.tmp`

- `open()` returns a writer bound to a temporary file (typically with a `.tmp` suffix). You must close the writer before calling `commit()`.
- `commit()` performs an atomic `rename(temp, final)` so either the old file or the new file is visible on disk.
- Prefer `execute(writer -> {…})` to ensure the correct order: open → write → close → commit.

Examples in code:
- `sorteddatafile/SortedDataFileWriterTx#open()` → `commit()` renames temp to final
- `unsorteddatafile/UnsortedDataFileWriterTx#open()` → `commit()` renames temp to final
- `datablockfile/DataBlockWriterTx#open()` → `commit()` renames temp to final
- `bloomfilter/BloomFilterWriterTx#open()` → `commit()` renames temp to final and swaps hash

## 🧭 Practical Guidance

- Call `flushAndWait()` on periodic boundaries and always before shutdown to persist in‑memory writes.
- After a crash, reopen the index and run `checkAndRepairConsistency()`; optionally trigger a `compact()` to collapse delta caches.
- Remember there is no WAL: durability is guaranteed at the `flushAndWait()`/close boundaries and via atomic file replacement for all data files.

## 🔗 Related Glossary

- [Flush](glossary.md#flush)
- [Write Transaction](glossary.md#write-transaction)
- [Compaction](glossary.md#compaction)
- [Consistency Checker](glossary.md#consistency-checker)
