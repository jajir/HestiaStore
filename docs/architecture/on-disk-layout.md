# On-Disk Layout & File Names

This page documents the files HestiaStore writes into an index directory, their naming conventions, how they evolve over time, and the atomic commit pattern used to keep them consistent.

## Directory Layout (One Index per Directory)

Top-level files:
- `index.map` — Global key→segment map (max key per segment). Sorted key→SegmentId pairs. Updated atomically.
- `wal-00000.log`, `wal-00001.log`, … — Optional context logs if enabled. Useful for observability; not a recovery WAL.

Per‑segment files for segment `segment-00000`:
- `segment-00000.index` — Main SST in chunked format (ChunkStoreFile). Holds sorted key/value entries in chunks.
- `segment-00000.scarce` — Sparse index (key→chunk start position) to accelerate probes into `.index`.
- `segment-00000.bloom-filter` — Bloom filter backing store for negative lookups.
- `segment-00000.properties` — Segment properties (counts, delta numbering).
- `segment-00000.cache` — Optional seed file for the delta cache overlay (may be absent).
- `segment-00000-delta-000.cache`, `segment-00000-delta-001.cache`, … — Per‑segment delta cache files created between compactions.

Notes:
- Segment ids are zero‑based and padded: `segment-00000`, `segment-00001`, …
- Delta file counters are padded to 3 digits.

## Naming and Extensions

- Main data: `.index` (chunked SST)
- Sparse index: `.scarce` (sorted key→int pointer)
- Bloom: `.bloom-filter`
- Segment metadata: `.properties`
- Delta/overlay: `.cache` (both seed cache and delta files)
- Key→segment map: `index.map`
- Context log: `wal-xxxxx.log`

Code: `segment/SegmentFiles.java`, `sst/KeySegmentCache.java`, `log/LogFileNamesManager.java`.

## Atomic Commit Pattern (`*.tmp` + rename)

All persistent writers follow the same pattern:
1) `openWriter()` returns a writer bound to a temporary file (usually `*.tmp`).
2) Close the writer to flush OS buffers.
3) `commit()` atomically renames the temp file to its final name.

Implications:
- A crash never exposes a partially written visible file. At restart, either the old file or the new file is present.
- Readers treat missing files as empty where applicable (e.g., no delta files ⇒ empty overlay).

Code pointers:
- Delta cache: `sorteddatafile/SortedDataFileWriterTx` (used by `SegmentDeltaCacheWriter`)
- Main SST: `chunkentryfile/ChunkEntryFileWriterTx` → `chunkstore/ChunkStoreWriterTx` → `datablockfile/DataBlockWriterTx`
- Sparse index: `scarceindex/ScarceIndexWriterTx`
- Bloom filter: `bloomfilter/BloomFilterWriterTx`
- Unsorted log: `unsorteddatafile/UnsortedDataFileWriterTx`

## Segment Lifecycle

1) New writes accumulate in the index write buffer; on flush they are routed by key into per‑segment delta files `segment-xxxxx-delta-YYY.cache`.
2) Reads consult delta cache first, then `.bloom-filter` and `.scarce` to bound the probe into `.index`.
3) Compaction rewrites `.index`, `.scarce`, and `.bloom-filter` transactionally; on success, delta files are deleted and the in‑memory delta cache is cleared.
4) When a segment grows beyond the threshold, it is split: a new `segment-xxxxx` appears and `index.map` is updated atomically.

## Chunked SST Anatomy

The `.index` file is a sequence of fixed‑cell chunks stored in a data‑block file. Each chunk has:
- Header: magic number, version, payload length, CRC32, flags
- Payload: a batch of sorted entries, optionally transformed by filters

Filters add robustness and optional compression/obfuscation; their flags and order are recorded so the reader can invert them. See “Filters & Integrity”.

Code: `chunkstore/*`, `chunkentryfile/*`.

## Compatibility

- Header fields (magic, version) allow future readers to validate format.
- Sparse index and Bloom filter are rebuilt during compaction; no upgrade step is required beyond re‑writing segments if formats change in the future.

## Example Directory (minimal)

```
index.map
segment-00000.index
segment-00000.scarce
segment-00000.bloom-filter
segment-00000.properties
segment-00000-delta-000.cache   # present until compaction
# wal-00000.log                 # only if context logging is enabled
```

## Related Glossary

- [SegmentId](glossary.md#segmentid)
- [Main SST](glossary.md#main-sst)
- [Sparse Index](glossary.md#sparse-index-scarce-index)
- [Bloom Filter](glossary.md#bloom-filter)
- [Key-to-Segment Map](glossary.md#key-to-segment-map)
- [Delta Cache](glossary.md#delta-cache)
- [Write Transaction](glossary.md#write-transaction)
