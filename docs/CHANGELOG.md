# 🗓️ Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/).

## 🏷️ 0.0.5

### ✨ Added

- Data blocks were introduced and the on-disk storage format was significantly improved.
- All disk writes now use a temporary file followed by an atomic rename; all streams are correctly closed.
- Data storage is configurable via the application configuration.
- Data in chunks and data blocks are validated using a magic number and CRC32.
- Added support for Snappy compression.

## 🏷️ 0.0.4

### ✨ Added

- Add recovery support to rebuild indexes after failures. ([#22](https://github.com/jajir/HestiaStore/issues/22))
- Introduce pages for segment-based indexing. ([#31](https://github.com/jajir/HestiaStore/issues/31))
- Create `Directory` implementation using `java.nio`. ([#50](https://github.com/jajir/HestiaStore/issues/50))
- Add a performance comparison framework for benchmark testing. ([#60](https://github.com/jajir/HestiaStore/issues/60))
- Add integration tests for deletion and graceful degradation. ([#76](https://github.com/jajir/HestiaStore/issues/76), [#63](https://github.com/jajir/HestiaStore/issues/63))
- Add a test class for long-running index operations. ([#49](https://github.com/jajir/HestiaStore/issues/49))

### 🔧 Changed

- Improve design of the `sorteddatafile` package for better modularity. ([#59](https://github.com/jajir/HestiaStore/issues/59))
- Enhance index configuration validation and parameter consistency. ([#81](https://github.com/jajir/HestiaStore/commit))
- Introduce limits on the number of delta files to prevent unbounded growth. ([#75](https://github.com/jajir/HestiaStore/issues/75))
