![HestiaStore logo](./images/logo.png)

[![Build (master)](https://github.com/jajir/HestiaStore/actions/workflows/maven.yml/badge.svg?branch=master)](https://github.com/jajir/HestiaStore/actions/workflows/maven.yml?query=branch%3Amain)
![test results](https://gist.githubusercontent.com/jajir/a613341fb9d9d0c6a426b42a714700b7/raw/badge-main.svg)
![line coverage](https://gist.githubusercontent.com/jajir/a613341fb9d9d0c6a426b42a714700b7/raw/jacoco-badge-main.svg)
![OWAPS dependency check](https://gist.githubusercontent.com/jajir/a613341fb9d9d0c6a426b42a714700b7/raw/badge-owasp-main.svg)
[![License: LGPL v3](https://img.shields.io/badge/License-LGPL%20v3-blue.svg)](https://www.gnu.org/licenses/lgpl-3.0)
[![OpenSSF Best Practices](https://www.bestpractices.dev/projects/10654/badge)](https://www.bestpractices.dev/projects/10654)
![Maven Central Version](https://img.shields.io/maven-central/v/org.hestiastore.index/core)
[![javadoc](https://javadoc.io/badge2/org.hestiastore.index/core/javadoc.svg)](https://javadoc.io/doc/org.hestiastore.index/core)
[![Quality Gate Status](https://sonarcloud.io/api/project_badges/measure?project=jajir_HestiaStore&metric=alert_status)](https://sonarcloud.io/summary/new_code?id=jajir_HestiaStore)
[![Bugs](https://sonarcloud.io/api/project_badges/measure?project=jajir_HestiaStore&metric=bugs)](https://sonarcloud.io/summary/new_code?id=jajir_HestiaStore)
[![Code Smells](https://sonarcloud.io/api/project_badges/measure?project=jajir_HestiaStore&metric=code_smells)](https://sonarcloud.io/summary/new_code?id=jajir_HestiaStore)

HestiaStore is a lightweight, embeddable key-value storage engine optimized for billions of records, designed to run in a single directory with high performance and minimal configuration.

Features:

```plaintext
 • Pure Java (no native dependencies), easy to embed
 • 200k+ ops/s; predictable I/O with configurable buffering
 • In-memory or file-backed storage, zero-config setup
 • Pluggable filters: Snappy compression, CRC32 integrity, magic-number validation
 • Bloom filter for fast negative lookups (tunable false-positive rate)
 • Segmented SST structure with sparse index for efficient range scans
 • Custom key/value types via type descriptors
 • Single-writer, multi-reader (optional synchronized mode)
 • Test-friendly MemDirectory for fast, isolated tests
 • Roadmap: write-ahead logging and advanced compaction
```

## 🚀 Performance Comparison

| Engine | Score [ops/s] | Occupied space | CPU Usage |
|:-------|--------------:|---------------:|----------:|
| ChronicleMap |         5 954 | 20.54 GB | 7% |
| H2 |        13 458 | 8 KB | 21% |
| HestiaStoreBasic |       208 723 | 9.71 GB | 6% |
| HestiaStoreCompress |       197 335 | 4.97 GB | 6% |
| LevelDB |        45 263 | 1.4 GB | 17% |
| MapDB |         2 946 | 496 MB | 14% |
| RocksDB |       305 712 | 7.74 GB | 6% |

Detailed methodology and full benchmark artifacts are available at [benchmark results](https://hestiastore.org/benchmark-results/).

## 📊 Feature Comparison

Architecture & Concurrency

| Engine | Storage/Index | Concurrency | Background Work |
|:--|:--|:--|:--|
| HestiaStore | Segmented on-disk structure | Single-writer, multi-reader (optional synchronized) | Periodic segment flush/merge |
| RocksDB | LSM tree (leveled/uni) | Highly concurrent | Compaction + flush threads |
| LevelDB | LSM tree | Single-writer, multi-reader | Compaction |
| MapDB | B-tree/H-tree | Thread-safe (synchronized) | Periodic commits |
| ChronicleMap | Off-heap mmap hash map | Lock-free/low-lock | None (no compaction) |
| H2 | B-tree | Concurrent (MVCC) | Checkpoint/auto-vacuum |

Durability & Fit

| Engine | Durability | Compression | Runtime Deps | Typical Fit |
|:--|:--|:--|:--|:--|
| HestiaStore | File-backed; commit on close | Supported | Pure Java (JAR-only) | Embedded KV with simple ops, large datasets |
| RocksDB | WAL + checkpoints (optional transactions) | Snappy/Zstd/LZ4 | Native library | High write throughput, low-latency reads |
| LevelDB | File-backed; no transactions | Snappy | JAR-only port/native bindings | Lightweight LSM, smaller footprints |
| MapDB | File-backed; optional TX | None/limited | Pure Java (JAR-only) | Simple embedded maps/sets |
| ChronicleMap | Memory-mapped persistence; no ACID TX | None | Pure Java (JAR-only) | Ultra-low latency shared maps |
| H2 | WAL + MVCC transactions | Optional | Pure Java (JAR-only) | SQL + transactional workloads |

Notes

- “Concurrency” describes the general access model; specifics depend on configuration and workload.
- HestiaStore focuses on predictable file I/O with configurable buffering; WAL/transactions are on the roadmap.

## 🤝 Contributing

We welcome contributions! Please read our [Contributing Guidelines](CONTRIBUTING.md) before submitting a pull request.

## 📚 Documentation

- [HestiaStore Index architecture](https://hestiastore.org/architecture/arch-index/)
- [How to use HestiaStore](https://hestiastore.org/how-to-use/) including some examples
- [Index configuration](https://hestiastore.org/configuration/) and configuration properties explaining
- [Library Logging](https://hestiastore.org/configuration/logging/) How to setup loggin
- [Project versioning and how to release](https://hestiastore.org/development/release/) snapshot and new version

<!--
* [Segment implementation details](segment.md)
-->

## 📦 Installation and Basic Usage

To include HestiaStore in your Maven project, add the following dependency to your `pom.xml`:

```xml
<dependencies>
  <dependency>
    <groupId>org.hestiastore.index</groupId>
    <artifactId>core</artifactId>
    <version>0.0.3</version>
  </dependency>
</dependencies>
```

Replace the version number with the latest available from Maven Central.

**Note**: HestiaStore requires Java 17 or newer.

You can create a new index using the builder pattern as shown below:

```java
// Create an in-memory file system abstraction
Directory directory = new MemDirectory();

// Prepare index configuration
IndexConfiguration<String, String> conf = IndexConfiguration
        .<String, String>builder()//
        .withKeyClass(String.class)//
        .withValueClass(String.class)//
        .withName("test_index") //
        .build();

// Create a new index
Index<String, String> index = Index.<String, String>create(directory, conf);

// Perform basic operations
index.put("Hello", "World");

String value = index.get("Hello");
System.out.println("Value for 'Hello': " + value);
```

## 🗺️ Roadmap

Planned improvements include:

- Enhance Javadoc documentation
- Implement data consistency verification using checksums
- Complete the implementation of Write-Ahead Logging (WAH)

For detailed tasks and progress, see the [GitHub Issues](https://github.com/jajir/HestiaStore/issues) page.

## ❓ Need Help or Have Questions?

If you encounter a bug, have a feature request, or need help using HestiaStore, please [create an issue](https://github.com/jajir/HestiaStore/issues).
