# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/).

## 0.0.6 (Unreleased)

### Changed

- Key encoding now uses a single-pass API (`TypeEncoder#encode(T, byte[])`) across read and write paths, including Bloom filter lookup, WAL encoding, and type writers.
- Added `EncodedBytes` as the shared return type for encoded payload (`bytes` + effective `length`).

### Breaking

- `TypeEncoder` no longer exposes `bytesLength(T)` and `toBytes(T, byte[])`.
- External/custom `TypeEncoder` implementations must migrate to:
  - `EncodedBytes encode(T value, byte[] reusableBuffer)`
- Migration intent:
  - Use the provided reusable buffer when it is large enough.
  - Allocate a larger buffer when needed.
  - Return effective encoded length in `EncodedBytes.length`.

## 0.0.5

### Added

- Data blocks were introduced and the on-disk storage format was significantly improved.
- All disk writes now use a temporary file followed by an atomic rename; all streams are correctly closed.
- Data storage is configurable via the application configuration.
- Data in chunks and data blocks are validated using a magic number and CRC32.
- Added support for Snappy compression.

## 0.0.4

### Added

- Add recovery support to rebuild indexes after failures. ([#22](https://github.com/jajir/HestiaStore/issues/22))
- Introduce pages for segment-based indexing. ([#31](https://github.com/jajir/HestiaStore/issues/31))
- Create `Directory` implementation using `java.nio`. ([#50](https://github.com/jajir/HestiaStore/issues/50))
- Add a performance comparison framework for benchmark testing. ([#60](https://github.com/jajir/HestiaStore/issues/60))
- Add integration tests for deletion and graceful degradation. ([#76](https://github.com/jajir/HestiaStore/issues/76), [#63](https://github.com/jajir/HestiaStore/issues/63))
- Add a test class for long-running index operations. ([#49](https://github.com/jajir/HestiaStore/issues/49))

### Changed

- Improve design of the `sorteddatafile` package for better modularity. ([#59](https://github.com/jajir/HestiaStore/issues/59))
- Enhance index configuration validation and parameter consistency. ([#81](https://github.com/jajir/HestiaStore/commit))
- Introduce limits on the number of delta files to prevent unbounded growth. ([#75](https://github.com/jajir/HestiaStore/issues/75))
