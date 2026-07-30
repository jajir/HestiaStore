# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/).

## 1.1.0 (Unreleased)

### Added

- Added bounded key-range scans through
  `SegmentIndex.scan(fromInclusive, toExclusive)` and
  `SegmentIndex.scan(fromInclusive, toExclusive, isolation)`.
- Added conditional write operations on `SegmentIndex`:
  `putIfAbsent(key, value)` and `replace(key, expectedValue, newValue)`.
- Added strict UTF-8 string descriptors for emoji and multilingual keys and
  values: `TypeDescriptorTinyUtf8String` for payloads up to 255 encoded bytes
  and `TypeDescriptorUtf8String` for larger payloads.
- Added a composite-key example to show descriptor-backed ordering for
  multi-part keys.
- Added benchmark coverage for bounded range scans, persisted mutations, hot
  route writes, and per-change benchmark history.

### Changed

- Improved point-operation synchronization, route leasing, WAL append
  concurrency, cache memory retention, and sorted cache snapshot allocation.
- Refreshed benchmark documentation and added more stable benchmark profile
  baselines for release-to-release performance review.
- Improved startup memory reporting and monitoring-console startup scripts.

### Breaking

- `SegmentIndex` now declares `putIfAbsent(...)` and `replace(...)`.
  External implementations of `SegmentIndex` must implement both methods.
- `SegmentIndex.scan(...)` is now part of the public API. Implementations that
  do not support bounded scans may keep the default unsupported behavior, but
  callers can now compile against the range-scan contract.

### Compatibility Notes

- Conditional writes are currently supported only when WAL is disabled. They
  throw `IndexException` when WAL is enabled.
- `SegmentIndex.scan(...)` uses half-open bounds: the lower bound is inclusive
  and the upper bound is exclusive. A `null` upper bound scans to the end.
- The default `String` descriptor remains ISO-8859-1 for on-disk
  compatibility. Use `TypeDescriptorTinyUtf8String` or
  `TypeDescriptorUtf8String` explicitly before creating indexes that must store
  emoji or multilingual text.

## 1.0.0 (2026-06-30)

### Added

- Published the first stable HestiaStore release to Maven Central.
- Published the multi-module release artifacts, including the engine,
  operational tools, monitoring bridges, source jars, and Javadocs.

### Changed

- Key encoding uses a single-pass API (`TypeEncoder#encode(T, byte[])`) across
  read and write paths, including Bloom filter lookup, WAL encoding, and type
  writers.
- Added `EncodedBytes` as the shared return type for encoded payload bytes and
  effective encoded length.

### Breaking

- `TypeEncoder` no longer exposes `bytesLength(T)` and `toBytes(T, byte[])`.
- External/custom `TypeEncoder` implementations must migrate to
  `EncodedBytes encode(T value, byte[] reusableBuffer)`.

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
