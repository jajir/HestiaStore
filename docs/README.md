![HestiaStore logo](./images/logo.png)

[![Build (master)](https://github.com/jajir/HestiaStore/actions/workflows/maven.yml/badge.svg?branch=master)](https://github.com/jajir/HestiaStore/actions/workflows/maven.yml?query=branch%3Amain)
![test results](https://gist.githubusercontent.com/jajir/a613341fb9d9d0c6a426b42a714700b7/raw/badge-main.svg)
![line coverage](https://gist.githubusercontent.com/jajir/a613341fb9d9d0c6a426b42a714700b7/raw/jacoco-badge-main.svg)
![OWAPS dependency check](https://gist.githubusercontent.com/jajir/a613341fb9d9d0c6a426b42a714700b7/raw/badge-owasp-main.svg)
[![License: LGPL v3](https://img.shields.io/badge/License-LGPL%20v3-blue.svg)](https://www.gnu.org/licenses/lgpl-3.0)
![Maven Central Version](https://img.shields.io/maven-central/v/org.hestiastore.index/core)

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

* [HestiaStore Index architecture](https://jajir.github.io/HestiaStore/architecture/)
* [How to use HestiaStore](https://jajir.github.io/HestiaStore/how-to-use-index/) including some examples
* [Index configuration](https://jajir.github.io/HestiaStore/configuration/) and configuration properties explaining
* [Library Logging](https://jajir.github.io/HestiaStore/logging/) How to setup loggin
* [Project versioning and how to release](https://jajir.github.io/HestiaStore/release/) snapshot and new version

<!--
* [Segment implementation details](segment.md)
-->

## 🚀 How to Use HestiaStore

At file `pom.xml` to section `dependencies` add the following:

```xml
<dependencies>
  <dependency>
    <groupId>org.hestiastore.index</groupId>
    <artifactId>core</artifactId>
    <version>0.0.3</version> <!-- Replace with the actual version -->
  </dependency>
</dependencies>
```

New Index should be created with builder, which make index instance. For example:

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

// create new index
Index<String, String> index = Index.<String, String>create(directory, conf);

// Do some work with the index
index.put("Hello", "World");

String value = index.get("Hello");
System.out.println("Value for 'Hello': " + value);
```

### ❓ Need Help or Have Questions?

If you encounter a bug, have a feature request, or need help using HestiaStore, please [create an issue](https://github.com/jajir/HestiaStore/issues).
