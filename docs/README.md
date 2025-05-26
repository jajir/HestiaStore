![HestiaStore logo](./images/logo.png)

[![Build (master)](https://github.com/jajir/HestiaStore/actions/workflows/maven.yml/badge.svg?branch=master)](https://github.com/jajir/HestiaStore/actions/workflows/maven.yml?query=branch%3Amain)
![test results](https://gist.githubusercontent.com/jajir/a613341fb9d9d0c6a426b42a714700b7/raw/badge-main.svg)
![line coverage](https://gist.githubusercontent.com/jajir/a613341fb9d9d0c6a426b42a714700b7/raw/jacoco-badge-main.svg)
![OWAPS dependency check](https://gist.githubusercontent.com/jajir/a613341fb9d9d0c6a426b42a714700b7/raw/badge-owasp-main.svg)
[![License: LGPL v3](https://img.shields.io/badge/License-LGPL%20v3-blue.svg)](https://www.gnu.org/licenses/lgpl-3.0)

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

## Documentation

* [HestiaStore Index architecture](architecture.md)
* [How to use HestiaStore](how-to-use-index.md) including some examples
* [Index configuration](configuration.md) and configuration properties explaining
* [Logging](logging.md) How to setup loggin
* [Project versioning and how to release](release.md) snapshot and new version
* [Security](SECURITY.md) and related topics
* [Change log](CHANGELOG.md)

<!--
* [Segment implementation details](segment.md)
-->

## How to use HestiaStore

Index should be created with builder, which make index instance. For example:

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
