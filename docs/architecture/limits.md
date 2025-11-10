# Limitations & Trade‑offs

This page lists the most important constraints and design trade‑offs so you can plan deployments and avoid surprises. Each item links to the code or related docs for verification.

## Durability and Recovery

- No WAL recovery: There is no write‑ahead log to replay after a crash. Durability boundaries are explicit `flush()` and `close()`. Writes still in the index write buffer (in‑memory `UniqueCache`) are not durable until flushed. See Recovery.
- Per‑file atomicity only: Writers use temp files + atomic rename; groups of files (e.g., SST + scarce index + bloom) commit in a safe order but not as a single atomic unit. Readers remain consistent because old files stay in place until each rename. See SegmentFullWriterTx, BloomFilterWriterTx.
- Filesystem requirement: Crash safety relies on same‑directory atomic `rename`. Use local filesystems; be cautious with network filesystems that may not guarantee strict atomicity.
- Stale lock files: A crash can leave `.lock` behind, preventing open until removed. See `directory/FsFileLock.java` and IndexState*. Remove the file only when certain no process still uses the directory.

## Concurrency

- Default build is not thread‑safe: `SstIndexImpl` favors throughput with no internal locking. Use one writer thread (and coordinate readers), or enable the synchronized variant.
- Thread‑safe mode uses a coarse `ReentrantLock`: `IndexInternalSynchronized` serializes all ops; long scans contend with writes. Iterators in this mode take/release the same lock per step.
- Optimistic iteration: Segment iterators may stop early if a write bumps the version during a scan (by design). Re‑open the iterator to continue. See `EntryIteratorWithLock`.

## Size and Addressing Limits

- Per‑segment SST size bounded by 32‑bit positions: Sparse index stores an `Integer` position and readers cast to `int` (`ChunkEntryFile#openIteratorAtPosition((int)position)`). Keep a single `.index` file below ~2 GiB. Use multiple segments to scale. Code: `chunkentryfile/ChunkEntryFile.java`, `scarceindex/*`.
- Data‑block and cell sizing constraints: `diskIoBufferSize` must be divisible by 1024; chunk cell size is fixed at 16 bytes. Payloads pad to whole cells (space overhead). Code: `Vldtn#requireIoBufferSize`, `chunkstore/CellPosition.java`.
- Log file rollover ceiling: Context logging keeps up to 99,999 files (`wal-xxxxx.log`). Hitting this throws. Code: `log/LogFileNamesManager`.

## Configuration Immutability

Once an index is created, several properties cannot be changed when reopening with a config:

- Type descriptors (key/value serialization)
- Sparse index cadence (`maxNumberOfKeysInSegmentChunk`)
- Segment sizing limits (e.g., `maxNumberOfKeysInSegment`)
- Bloom filter sizing and hash functions
- Encoding/decoding filter lists (order and membership)

Attempts to change these raise an error in `IndexConfigurationManager.validateThatWasntChanged`. To change them, create a new index and bulk‑copy data (read + write) or export/import. See `sst/IndexConfigurationManager.java`.

## Data Model and Semantics

- Strictly increasing keys: All on‑disk structures assume ascending order; compaction and consistency checks enforce it. Incorrect comparators or inconsistent key ordering will fail. Code: `segment/SegmentConsistencyChecker.java`.
- Tombstones: Deletes are tombstones until compaction merges and drops them. Heavy delete workloads without compaction may grow delta files and increase read work.
- No multi‑key transactions: Writes are per‑key; there is no cross‑key atomic batch. Use application‑level coordination if needed.

## Security Posture

- XOR filter is not encryption: `ChunkFilterXorEncrypt` provides reversible obfuscation only; do not use as security. For encryption at rest, place HestiaStore on an encrypted volume or add a real crypto layer above. See `chunkstore/ChunkFilterXor*`.
- No authentication/authorization: HestiaStore is an embedded library and relies on your process/container isolation.

## Workloads That Fit Well

- High‑throughput append/update workloads where read‑after‑write visibility matters and periodic flush/compaction is acceptable.
- Point lookups and ordered scans with predictable latency (Bloom + sparse index bound I/O).

## Anti‑patterns

- Expecting durability without flush/close.
- Relying on WAL replay (not implemented).
- Very large single segments (>2 GiB `.index`); split into more segments.
- Heavy mixed concurrent reads/writes with strict low‑latency tail in synchronized mode (coarse locking).

## Mitigations and Best Practices

- Plan periodic `flush()` and `compact()` windows; after crashes run consistency check and optionally compact.
- Size Bloom filters for your negative‑lookup rate; monitor `BloomFilterStats`.
- Tune `maxNumberOfKeysInSegmentChunk` to balance read scan length vs. sparse index size.
- Use multiple segments to stay under per‑segment limits and to improve compaction parallelism (future).

## Related Docs

- Recovery: `architecture/recovery.md`
- Concurrency: `architecture/concurrency.md`
- Filters & Integrity: `architecture/filters.md`
- On‑Disk Layout: `architecture/on-disk-layout.md`

