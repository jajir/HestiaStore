# Alternatives

Use this page when you are deciding between HestiaStore and other embedded
storage engines. The goal is not to declare a universal winner, but to make the
trade-offs visible quickly.

## Quick comparison

### Architecture and concurrency

| Engine | Storage or index model | Concurrency model | Background work |
| :-- | :-- | :-- | :-- |
| HestiaStore | Segmented on-disk structure | Thread-safe concurrent operations with segment-level parallelism | Periodic segment flush and merge |
| RocksDB | LSM tree | Highly concurrent | Compaction and flush threads |
| LevelDB | LSM tree | Single-writer, multi-reader | Compaction |
| MapDB | B-tree or H-tree | Thread-safe, commonly synchronized | Periodic commits |
| Chronicle Map | Off-heap memory-mapped hash map | Low-lock or lock-free style | None |
| H2 MVStore | B-tree style storage engine | Concurrent with MVCC-style behavior | Checkpoint and auto-vacuum |

### Durability and fit

| Engine | Durability model | Runtime deps | Typical fit |
| :-- | :-- | :-- | :-- |
| HestiaStore | File-backed with flush or close boundaries, optional WAL for local crash recovery | Pure Java | Embedded KV with predictable local durability and ordered scans |
| RocksDB | WAL, checkpoints, broad durability tuning | Native library plus Java binding | High write throughput and heavily tuned storage workloads |
| LevelDB | File-backed with compaction, limited transactional semantics | Java port or native binding | Lightweight LSM use cases with simple persistence needs |
| MapDB | File-backed collections with optional transactional features | Pure Java | Java collection-like persistence for maps and sets |
| Chronicle Map | Memory-mapped persistence without ACID transactions | Pure Java | Very low-latency shared maps and off-heap access |
| H2 MVStore | WAL plus transactional database behavior through H2 | Pure Java | Embedded relational or transactional application storage |

Notes:

- These rows are directional summaries, not formal benchmarks or guarantees.
- Concurrency, durability, and background behavior depend on actual
  configuration and workload.
- For measured throughput comparisons, use [Benchmarks](benchmarks.md).

## When HestiaStore fits best

HestiaStore is a strong choice when you want:

- a pure-Java embedded engine with no native dependency
- point lookups plus ordered scans over large local datasets
- explicit operational control over WAL, backup, and monitoring
- simpler local deployment than a separate database service

It is a weaker fit when you primarily need:

- distributed replication
- SQL and relational querying
- high-end native-engine write throughput regardless of native dependencies
- full transactional semantics across many keys

## Alternative notes

### MapDB

[Homepage](https://mapdb.org/) / [GitHub](https://github.com/jankotek/mapdb)

MapDB is attractive when you want disk-backed Java collections with a familiar
API shape. It is a closer fit to persisted maps and sets than to a storage
engine that emphasizes on-disk structure control and operational tooling.

### H2 MVStore

[Homepage](https://www.h2database.com/html/main.html)

H2 MVStore is a better fit when the application is already close to H2 or needs
transactional database behavior rather than a dedicated embedded KV engine.

### Chronicle Map

[Homepage](https://github.com/OpenHFT/Chronicle-Map)

Chronicle Map is compelling for very low-latency, off-heap, memory-mapped data
sharing. It is less focused than HestiaStore on ordered on-disk segment
structure and operational runbooks.

### RocksDB

[Homepage](https://rocksdb.org/) / [GitHub](https://github.com/facebook/rocksdb/)

RocksDB is the obvious comparison when maximum write throughput and deep LSM
tuning matter more than avoiding native dependencies or keeping operational
surface smaller.

### LevelDB

LevelDB remains a useful mental comparison for simple LSM-based local storage.
It is less feature-rich operationally than RocksDB and offers a different set
of trade-offs than HestiaStore's segment-oriented layout.

### BabuDB

[Homepage](https://github.com/xtreemfs/babudb)

BabuDB is less common today, but still interesting historically as a
write-ahead-log-oriented embedded key-value design. It is usually a niche
comparison rather than the primary decision point for new adopters.
