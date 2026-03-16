# On-Disk Layout & File Names

This page documents the files HestiaStore writes into an index directory, their naming conventions, how they evolve over time, and the atomic commit pattern used to keep them consistent.

## Directory Layout (One SegmentIndex per Directory)

Top-level files:

- `index.map` — Global key→segment map (max key per segment). Sorted key→SegmentId pairs. Updated atomically.

Top-level directories:

- `segment-00000/`, `segment-00001/`, … — one directory per segment id.

Per‑segment files (inside `segment-00000/`):

- `manifest.txt` — Segment metadata (counts, active version, delta numbering).
- `.lock` — Segment lock file.
- `v01-index.sst` — Main SST in chunked format (ChunkStoreFile). Holds sorted key/value entries in chunks.
- `v01-scarce.sst` — Sparse index (key→chunk start position) to accelerate probes into the main SST.
- `v01-bloom-filter.bin` — Bloom filter backing store for negative lookups.
- `v01-delta-0000.cache`, `v01-delta-0001.cache`, … — Per‑segment delta cache files created between compactions.

Notes:

- Segment ids are zero‑based and padded: `segment-00000`, `segment-00001`, …
- Versions are zero‑padded to 2 digits: `v01`, `v02`, …
- Delta file counters are padded to 4 digits: `0000`, `0001`, …

## Naming and Extensions

- Main data: `vNN-index.sst` (chunked SST)
- Sparse index: `vNN-scarce.sst` (sorted key→int pointer)
- Bloom: `vNN-bloom-filter.bin`
- Segment metadata: `manifest.txt`
- Segment lock: `.lock`
- Delta/overlay: `vNN-delta-NNNN.cache`
- Key→segment map: `index.map`

Code: `segment/SegmentFiles.java`, `segmentindex/mapping/KeyToSegmentMap.java`.

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

## Segment Lifecycle

1) New writes accumulate in the index write buffer; on flush they are routed by key into per‑segment delta files `vNN-delta-NNNN.cache`.
2) Reads consult delta cache first, then `vNN-bloom-filter.bin` and `vNN-scarce.sst` to bound the probe into `vNN-index.sst`.
3) Compaction rewrites `vNN-index.sst`, `vNN-scarce.sst`, and `vNN-bloom-filter.bin` transactionally; on success, delta files are deleted and the in‑memory delta cache is cleared.
4) When a segment grows beyond the threshold, it is split: a new `segment-xxxxx` appears and `index.map` is updated atomically.

## Chunked SST Anatomy

The `vNN-index.sst` file is a sequence of fixed‑cell chunks stored in a data‑block file. Each chunk has:

- Header: magic number, version, payload length, CRC32, flags
- Payload: a batch of sorted entries, optionally transformed by filters

Filters add robustness and optional compression/obfuscation; their flags and order are recorded so the reader can invert them. See “Filters & Integrity”.

Code: `chunkstore/*`, `chunkentryfile/*`.

## Compatibility

- Header fields (magic, version) allow future readers to validate format.
- Sparse index and Bloom filter are rebuilt during compaction; no upgrade step is required beyond re‑writing segments if formats change in the future.

## Example Directory (minimal)

```text
index.map
segment-00000/
  manifest.txt
  .lock
  v01-index.sst
  v01-scarce.sst
  v01-bloom-filter.bin
  v01-delta-0000.cache   # present until compaction
```

## Related Glossary

- [SegmentId](../general/glossary.md#segmentid)
- [Main SST](../general/glossary.md#main-sst)
- [Sparse Index](../general/glossary.md#sparse-index-scarce-index)
- [Bloom Filter](../general/glossary.md#bloom-filter)
- [Key-to-Segment Map](../general/glossary.md#key-to-segment-map)
- [Delta Cache](../general/glossary.md#delta-cache)
- [Write Transaction](../general/glossary.md#write-transaction)
