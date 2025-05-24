![HestiaStore logo](./images/logo.png)

[![Build (master)](https://github.com/jajir/HestiaStore/actions/workflows/maven.yml/badge.svg?branch=master)](https://github.com/jajir/HestiaStore/actions/workflows/maven.yml?query=branch%3Amain)
![test results](https://gist.githubusercontent.com/jajir/a613341fb9d9d0c6a426b42a714700b7/raw/badge-main.svg)
![line coverage](https://gist.githubusercontent.com/jajir/a613341fb9d9d0c6a426b42a714700b7/raw/jacoco-badge-main.svg)
![OWAPS dependency check](https://gist.githubusercontent.com/jajir/a613341fb9d9d0c6a426b42a714700b7/raw/badge-owasp-main.svg)

Goal is to provide easy to use key value map for billions of records using just one directory and some space.

It's simple fast index. Work with index should be split into phases of:

* Writing data to index. All data that should be stored in index should be send to index.
* Building index. In this phase data are organized for fast access.
* Search through index. In this phase it's not possible to alter data in index.

Index is not thread safe.

## Documentation

* [HestiaStore Index architecture](architecture.md)
* [How to use HestiaStore](how-to-use-index.md) including some examples
* [Index configuration](configuration.md) and configuration properties explaining
* [Logging](logging.md) How to setup loggin
* [Project versioning and how to release](release.md) snapshot and new version
* [Security](SECURITY.md) and related topics

<!--
* [Segment implementation details](segment.md)
-->

## How to use HestiaStore

Index should be created with builder, which make index instance. For example:

```java
// Create an in-memory file system abstraction
final Directory directory = new MemDirectory();

// Prepare index configuration
final IndexConfiguration<String, String> conf = IndexConfiguration
        .<String, String>builder()//
        .withKeyClass(String.class)//
        .withValueClass(String.class)//
        .withName("test_index") //
        .build();

// create new index
Index<String, String> index = Index.<String, String>create(directory,
        conf);

// Do some work with the index
index.put("Hello", "World");

String value = index.get("Hello");
System.out.println("Value for 'Hello': " + value);
```
