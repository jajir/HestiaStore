# 🧪 Filters & Integrity

HestiaStore persists segment data in chunked files. Each chunk carries a small header and a payload processed by an ordered filter pipeline. Filters provide integrity (magic number, CRC32), optional compression, and optional reversible transformations. The same concept exists on both the write path (encoding pipeline) and read path (decoding pipeline).

This page summarizes what the filters do, how they are ordered, and how to configure them.

## 🏷️ Chunk Header and Flags

Every chunk has a header with these fields:

- Magic number — constant identifying HestiaStore chunk format
- Version — current format version (presently 1)
- Payload length — number of payload bytes (unpadded)
- CRC32 — checksum of payload bytes (see ordering recommendations below)
- Flags — bit field describing which filters transformed the payload

Flag bit positions (see `src/main/java/org/hestiastore/index/chunkstore/ChunkFilter.java`):

- 0 — magic number present
- 1 — CRC32 present (bit reserved; validation uses the header value)
- 3 — Snappy compression
- 4 — XOR encryption (reversible obfuscation)

## ✍️ Encoding Pipeline (Write Path)

Write path constructs a `ChunkData` and passes it through a `ChunkProcessor` configured with encoding filters. The writer then combines the resulting header and (possibly transformed) payload and writes padded bytes to the underlying cell store.

- Implementation: `chunkstore/ChunkStoreWriterImpl#write`
- Pipeline wrapper: `chunkstore/ChunkProcessor` with encoding filters
- Typical defaults: CRC32 → MagicNumber
- With compression/encryption enabled, recommended order:
  - CRC32 writing → Magic number writing → Snappy compression → XOR encrypt

Why this order:
- CRC32 computed on the plaintext payload gives a strong data‑integrity check after decoding (you must decompress/decrypt before CRC validation on read).
- Magic‑number header flag is a quick consistency guard before attempting other transforms.

## 📖 Decoding Pipeline (Read Path)

Read path pulls a raw chunk, parses the header, then applies the decoding filters in order. The final `ChunkData` is used to rebuild a consistent `Chunk` instance with the validated header and payload.

- Implementation: `chunkstore/ChunkStoreReaderImpl#read`
- Pipeline wrapper: `chunkstore/ChunkProcessor` with decoding filters
- Typical defaults: MagicNumber validation → CRC32 validation
- With compression/encryption enabled, recommended order:
  - MagicNumber validation → XOR decrypt → Snappy decompress → CRC32 validation

Notes:
- Validation filters check the corresponding header flag (when provided) and throw an exception if the precondition fails (e.g., “not marked as compressed”).
- CRC validation recomputes CRC32 on the current payload and compares to the header value.

## 🧰 Available Filters

Integrity
- Magic number writing/validation: `ChunkFilterMagicNumberWriting`, `ChunkFilterMagicNumberValidation`
  - Sets and validates the fixed magic number; toggles bit 0 in flags.
- CRC32 writing/validation: `ChunkFilterCrc32Writing`, `ChunkFilterCrc32Validation`
  - Computes/stores CRC over the payload; validation recomputes and compares.

Compression
- Snappy compress/decompress: `ChunkFilterSnappyCompress`, `ChunkFilterSnappyDecompress`
  - Fast compression; sets/clears bit 3 in flags.

Transformations
- XOR encrypt/decrypt: `ChunkFilterXorEncrypt`, `ChunkFilterXorDecrypt`
  - Lightweight reversible obfuscation; sets/clears bit 4 in flags.

Utility
- No‑op: `ChunkFilterDoNothing` (testing/bench harnesses)

## ⚙️ Configuration

Filters are configured on the index through the fluent builder and then stored in the index configuration:

- API: `segmentindex/IndexConfigurationBuilder`
  - `addEncodingFilter(ChunkFilter)` / `addEncodingFilter(Class<? extends ChunkFilter>)`
  - `addDecodingFilter(ChunkFilter)` / `addDecodingFilter(Class<? extends ChunkFilter>)`
  - `withEncodingFilters(Collection<ChunkFilter>)`
  - `withDecodingFilters(Collection<ChunkFilter>)`
- Defaults (when you don’t specify any):
  - Encoding: CRC32 writing → Magic number writing
  - Decoding: Magic number validation → CRC32 validation

The filter sequences are propagated into segment I/O via `SegmentFiles`, used by:
- Writer side: `ChunkStoreWriterTx` → `ChunkStoreWriterImpl`
- Reader side: `ChunkStoreReaderImpl`

Constraints:
- Filter lists must not be empty; `ChunkProcessor` enforces this.
- Decoding order must mirror the inverse of encoding for transforms like compression/encryption. If you enable Snappy or XOR, include the matching decode filters in the correct order.

## 🛡️ Error Handling and Safety

- Validation failures (wrong magic, CRC mismatch, missing flags) throw exceptions and abort the read; no partial state is committed.
- Writes are protected by transactional temp‑file + atomic rename; a failed write never exposes a partially written chunk to readers.

## 📋 Examples

Enable Snappy compression with correct decode order:

```java
IndexConfiguration<Integer, String> conf = IndexConfiguration.<Integer, String>builder()
    // ... types and other settings ...
    .addEncodingFilter(new ChunkFilterCrc32Writing())
    .addEncodingFilter(new ChunkFilterMagicNumberWriting())
    .addEncodingFilter(new ChunkFilterSnappyCompress())
    .addDecodingFilter(new ChunkFilterMagicNumberValidation())
    .addDecodingFilter(new ChunkFilterSnappyDecompress())
    .addDecodingFilter(new ChunkFilterCrc32Validation())
    .build();
```

Add XOR obfuscation on top of compression:

```java
builder
  .addEncodingFilter(new ChunkFilterXorEncrypt())
  .addDecodingFilter(new ChunkFilterXorDecrypt());
```

## 🧩 Code Pointers

- Pipeline engine: `src/main/java/org/hestiastore/index/chunkstore/ChunkProcessor.java`
- Filters: `src/main/java/org/hestiastore/index/chunkstore/ChunkFilter*.java`
- Writer path: `src/main/java/org/hestiastore/index/chunkstore/ChunkStoreWriterImpl.java`
- Reader path: `src/main/java/org/hestiastore/index/chunkstore/ChunkStoreReaderImpl.java`
- Configuration defaults: `src/main/java/org/hestiastore/index/segmentindex/IndexConfigurationContract.java`

## 🔗 Related Glossary

- [Filters](glossary.md#filters-chunk-filters)
- [Chunk](glossary.md#chunk)
- [Main SST](glossary.md#main-sst)
