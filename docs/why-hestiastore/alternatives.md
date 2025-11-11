# 🔀 Alternatives

HestiaStore is one of many available solutions for key-value storage. When selecting the right tool, it's important to consider which one best fits your needs. Here are some key evaluation criteria:

- 🔁 Transactional Support
- 🧪 ACID Compliance
- ☁️ Cloud Availability
- ⚡ Performance
- 🛠️ Error Handling
- 📚 API Completeness
...

Below are a few notable alternatives (not an exhaustive list):


## 🗺️ MapDB
[Homepage](https://mapdb.org/) / [GitHub](https://github.com/jankotek/mapdb)

MapDB focuses on replacing `java.util.Map` with a disk-backed map structure. While powerful, its recent versions have limited disk persistence support and performance may be slower for some use cases.


## 🗃️ H2 MVStore
[Homepage](https://www.h2database.com/html/main.html)

MVStore is the underlying storage engine for the H2 database. It features a friendly API, support for transactions, and generally good performance. It is well-suited for embedded systems and relational data scenarios.


## 📘 Chronicle Map
[Homepage](https://github.com/OpenHFT/Chronicle-Map)

Chronicle Map offers low-latency, off-heap key-value storage with support for huge datasets. It is especially suitable for high-performance and low-GC scenarios. Disk persistence is supported, though the primary target is memory-mapped data sharing.


## 🪨 RocksDB
[Homepage](https://rocksdb.org/) / [GitHub](https://github.com/facebook/rocksdb/)

RocksDB is a mature, high-performance embedded key-value store developed by Facebook. While it is written in C++, a Java binding is available. It supports compression, compaction, snapshots, and many tuning options.


## 🐘 BabuDB
[Homepage](https://github.com/xtreemfs/babudb)

BabuDB is a log-structured, non-relational key-value store optimized for write performance and reliability. It's less widely used today but offers interesting architectural choices like write-ahead logging and on-disk persistence.
