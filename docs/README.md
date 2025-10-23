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

```
 • Java-based with minimum external dependencies
 • Requires Java 17+
 • In-memory or file-backed indexes
 • Optional write-ahead logging
 • Supports user-defined key/value types
 • Optionaly could be thread safe
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

## 📦 Feature Comparison

| Engine               | Persistence Type    | Compression | Transactions | Concurrency Model   | Dependencies     | Index Structure |
|-----------------------|--------------------|--------------|---------------|---------------------|------------------|------------------|
| **HestiaStoreBasic**     | Append-log segments | ⚙️ Optional | ❌ No          | 🧵 Multi-threaded    | 📦 None (JAR-only) | 🌲 Segment tree   |
| **HestiaStoreCompress**  | Append-log segments | ✅ Yes      | ❌ No          | 🧵 Multi-threaded    | 📦 None (JAR-only) | 🌲 Segment tree   |
| **RocksDB**              | LSM-tree          | ✅ Yes      | ⚙️ Optional    | ⚡ Highly concurrent | 🧩 Native library  | 🪜 LSM levels     |
| **LevelDB**              | LSM-tree (Java)   | ✅ Yes      | ❌ No          | 🔀 Moderate          | 📦 None (JAR-only) | 🪜 LSM levels     |
| **MapDB**                | B-tree            | ❌ No       | ⚙️ Optional    | 🧱 Thread-safe       | 📦 None (JAR-only) | 🌳 B-tree         |
| **ChronicleMap**         | Memory-mapped     | ❌ No       | ❌ No          | 🔓 Lock-free         | 📦 None (JAR-only) | 🗺️ Hash map       |
| **H2**                   | B-tree (SQL)      | ⚙️ Optional | ✅ Yes         | 🔁 Concurrent        | 📦 None (JAR-only) | 🌳 B-tree         |

## 🤝 Contributing

We welcome contributions! Please read our [Contributing Guidelines](CONTRIBUTING.md) before submitting a pull request.

## 📚 Documentation

* [HestiaStore Index architecture](https://hestiastore.org/architecture/arch-index/)
* [How to use HestiaStore](https://hestiastore.org/how-to-use/) including some examples
* [Index configuration](https://hestiastore.org/configuration/) and configuration properties explaining
* [Library Logging](https://hestiastore.org/configuration/logging/) How to setup loggin
* [Project versioning and how to release](https://hestiastore.org/development/release/) snapshot and new version

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

* Enhance Javadoc documentation
* Implement data consistency verification using checksums
* Complete the implementation of Write-Ahead Logging (WAH)

For detailed tasks and progress, see the [GitHub Issues](https://github.com/jajir/HestiaStore/issues) page.

## ❓ Need Help or Have Questions?

If you encounter a bug, have a feature request, or need help using HestiaStore, please [create an issue](https://github.com/jajir/HestiaStore/issues).
