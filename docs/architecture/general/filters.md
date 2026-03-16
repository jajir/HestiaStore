# Filters & Integrity

HestiaStore persists segment data in chunked files. Each chunk carries a header and a payload processed by an ordered filter pipeline. Filters provide integrity (magic number, CRC32), optional compression, and optional reversible transformations.

This page focuses on filter behavior and ordering. For byte-level block/chunk
structure see [Data Block Format](datablock.md). For builder setup and examples,
see [Filter Configuration](../../configuration/filters.md).

## Chunk Flags Used by Filters

Chunk headers include `magic`, `version`, `payloadLength`, `crc32`, and `flags`.
This page documents only the `flags` bits used by filter pipeline steps.
Header field layout is documented in [Data Block Format](datablock.md).

Flag bit positions (see `src/main/java/org/hestiastore/index/chunkstore/ChunkFilter.java`):

- 0 — magic number present
- 1 — CRC32 present (bit reserved; validation uses the header value)
- 3 — Snappy compression
- 4 — XOR encryption (reversible obfuscation)

## Encoding Pipeline (Write Path)

Write path constructs a `ChunkData` and passes it through a `ChunkProcessor` configured with encoding filters. The writer then combines the resulting header and (possibly transformed) payload and writes padded bytes to the underlying cell store.

- Implementation: `chunkstore/ChunkStoreWriterImpl#writeSequence`
- Pipeline wrapper: `chunkstore/ChunkProcessor` with encoding filters
- Typical defaults: CRC32 → MagicNumber
- With compression/encryption enabled, recommended order:
  - CRC32 writing → Magic number writing → Snappy compression → XOR encrypt

Why this order:
- CRC32 computed on the plaintext payload gives a strong data‑integrity check after decoding (you must decompress/decrypt before CRC validation on read).
- Magic‑number header flag is a quick consistency guard before attempting other transforms.

## Decoding Pipeline (Read Path)

Read path pulls a raw chunk, parses the header, then applies the decoding filters in order. The final `ChunkData` is used to rebuild a consistent `Chunk` instance with the validated header and payload.

- Implementation: `chunkstore/ChunkStoreReaderImpl#read`
- Pipeline wrapper: `chunkstore/ChunkProcessor` with decoding filters
- Typical defaults: MagicNumber validation → CRC32 validation
- With compression/encryption enabled, recommended order:
  - MagicNumber validation → XOR decrypt → Snappy decompress → CRC32 validation

Notes:
- Validation filters check the corresponding header flag (when provided) and throw an exception if the precondition fails (e.g., “not marked as compressed”).
- CRC validation recomputes CRC32 on the current payload and compares to the header value.

## Available Filters

### Magic Number

1. What it does: writes and validates the canonical chunk magic marker
   (flag bit `0`) so readers can quickly detect invalid or unexpected chunk
   content.
2. Why it is valuable: provides a fast format guardrail before deeper decoding
   work.
3. Used classes: `ChunkFilterMagicNumberWriting`,
   `ChunkFilterMagicNumberValidation`.
4. External resources: [Magic number (programming)](https://en.wikipedia.org/wiki/Magic_number_(programming)).

### CRC32

1. What it does: computes CRC32 on write and validates CRC32 on read against
   payload bytes.
2. Why it is valuable: detects data corruption and payload mismatch early.
3. Used classes: `ChunkFilterCrc32Writing`, `ChunkFilterCrc32Validation`.
4. External resources: [Cyclic redundancy check](https://en.wikipedia.org/wiki/Cyclic_redundancy_check).

### Snappy Compression

1. What it does: compresses payload on write and decompresses on read (flag bit
   `3`).
2. Why it is valuable: reduces storage footprint and I/O volume with low
   latency overhead.
3. Used classes: `ChunkFilterSnappyCompress`, `ChunkFilterSnappyDecompress`.
4. External resources: [snappy-java](https://github.com/xerial/snappy-java).

### XOR Encryption

1. What it does: applies a reversible XOR transformation on write and restores
   bytes on read (flag bit `4`).
2. Why it is valuable: provides lightweight reversible obfuscation for specific
   local use cases.
3. Used classes: `ChunkFilterXorEncrypt`, `ChunkFilterXorDecrypt`.
4. External resources: [XOR cipher](https://en.wikipedia.org/wiki/XOR_cipher).

### Do Nothing Filter

1. What it does: passes chunk data through unchanged.
2. Why it is valuable: useful for tests, benchmarks, and pipeline wiring checks
   when you want zero transformation.
3. Used classes: `ChunkFilterDoNothing`.
4. External resources: none.

## Configuration

Filter setup, defaults, constraints, and code examples are documented in
[Filter Configuration](../../configuration/filters.md).

## Error Handling and Safety

- Validation failures (wrong magic, CRC mismatch, missing flags) throw exceptions and abort the read; no partial state is committed.
- Chunk data is written through transactional temp-file + rename (`DataBlockWriterTx`); broader crash/recovery semantics are documented in [Consistency & Recovery](recovery.md).
