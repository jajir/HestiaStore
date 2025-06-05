![HestiaStore logo](./images/logo.png)

[![Build (master)](https://github.com/jajir/HestiaStore/actions/workflows/maven.yml/badge.svg?branch=master)](https://github.com/jajir/HestiaStore/actions/workflows/maven.yml?query=branch%3Amain)
![test results](https://gist.githubusercontent.com/jajir/a613341fb9d9d0c6a426b42a714700b7/raw/badge-main.svg)
![line coverage](https://gist.githubusercontent.com/jajir/a613341fb9d9d0c6a426b42a714700b7/raw/jacoco-badge-main.svg)
![OWAPS dependency check](https://gist.githubusercontent.com/jajir/a613341fb9d9d0c6a426b42a714700b7/raw/badge-owasp-main.svg)
[![License: LGPL v3](https://img.shields.io/badge/License-LGPL%20v3-blue.svg)](https://www.gnu.org/licenses/lgpl-3.0)
[![OpenSSF Best Practices](https://www.bestpractices.dev/projects/10654/badge)](https://www.bestpractices.dev/projects/10654)
![Maven Central Version](https://img.shields.io/maven-central/v/org.hestiastore.index/core)
[![javadoc](https://javadoc.io/badge2/org.hestiastore.index/core/javadoc.svg)](https://javadoc.io/doc/org.hestiastore.index/core)

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
