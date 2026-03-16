# Filter Configuration

This page covers how to configure chunk filter pipelines on `IndexConfiguration`.

For filter behavior, ordering rationale, and integrity semantics, see
[Filters & Integrity](../architecture/general/filters.md).

## Builder API

Filters are configured through `IndexConfigurationBuilder`:

- `addEncodingFilter(ChunkFilter)` / `addEncodingFilter(Class<? extends ChunkFilter>)`
- `addDecodingFilter(ChunkFilter)` / `addDecodingFilter(Class<? extends ChunkFilter>)`
- `withEncodingFilters(Collection<ChunkFilter>)`
- `withDecodingFilters(Collection<ChunkFilter>)`

## Defaults

If you do not provide custom filters:

- Encoding defaults: `ChunkFilterCrc32Writing` -> `ChunkFilterMagicNumberWriting`
- Decoding defaults: `ChunkFilterMagicNumberValidation` -> `ChunkFilterCrc32Validation`

## Runtime Wiring

Configured filter lists are propagated into segment I/O:

- Writer side: `ChunkStoreWriterTx` -> `ChunkStoreWriterImpl`
- Reader side: `ChunkStoreReaderImpl`

## Constraints

- Encoding and decoding filter lists must not be empty.
- Decoding order must mirror the inverse of encoding for reversible transforms
  (for example Snappy/XOR pairs).

## Examples

Enable Snappy compression with matching decode order:

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

Snappy project home: [snappy-java](https://github.com/xerial/snappy-java)
