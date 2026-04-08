# Benchmark for 'Single-thread read' operation

## Chart

![Single-thread read benchmark chart](../images/out-read.svg)

## Percentile Chart

This chart shows the latency percentile curve for the benchmarked engines. The X axis runs from p50 to p99.99, and the Y axis uses a logarithmic latency scale so tail-latency differences are easier to compare.

![Single-thread read latency percentile chart](../images/out-read-percentiles.svg)

## Test Conditions - Single-thread Read Benchmarks

- Read-focused runs reuse the same controlled JVM, hardware, and JVM flag configuration as the write suite. Each trial prepares a clean directory pointed to by the `dir` system property before preloading the dataset.
- Setup inserts 10 000 000 deterministic key/value pairs (seed `324432L`) so every engine serves identical data. Keys come from `HashDataProvider`, while values remain the constant string `"opice skace po stromech"`.
- Warm-up iterations issue random lookups (80 % hits, 20 % misses) to trigger JIT compilation, cache population, and to ensure index structures have settled before measurements start.
- Each run exposes the same single-threaded read loop in two JMH modes: `SampleTime` to capture per-operation latency distribution and `Throughput` to capture sustained lookup performance over 20-second windows.
- Each benchmark keeps a consistent random sequence per iteration, ensuring engines experience the same access pattern and allowing apples-to-apples comparisons.
- After measurements finish, readers close their resources but the populated directories remain on disk so sizes can be captured by the reporting scripts.
- Tests executed on Mac mini 2024, 16 GB RAM, macOS 15.6.1 (24G90).

## Data for Throughtput Chart

| Engine | Score [ops/s] | Mean [us/op] | p50 [us/op] | p95 [us/op] | p99 [us/op] | Occupied space | CPU Usage |
|:-------|--------------:|-------------:|------------:|------------:|------------:|---------------:|----------:|
| ChronicleMap |     2 217 421 | 0.463 | 0.459 | 0.583 | 0.625 | 2.03 GB | 8% |
| H2 |       918 605 | 1.069 | 1.042 | 1.416 | 1.624 | 8 KB | 8% |
| LevelDB |       249 702 | 4.009 | 4.04 | 4.872 | 6.576 | 363.32 MB | 9% |
| MapDB |       192 366 | 5.218 | 5.328 | 6.328 | 6.664 | 1.3 GB | 6% |
| RocksDB |       158 462 | 6.266 | 6.248 | 8.208 | 8.912 | 324.24 MB | 8% |

## Source Data for Percentile Chart

| Engine | p50 [us/op] | p75 [us/op] | p90 [us/op] | p95 [us/op] | p99 [us/op] | p99.5 [us/op] | p99.9 [us/op] | p99.99 [us/op] |
|:-------|-------------:|-------------:|-------------:|-------------:|-------------:|-------------:|-------------:|-------------:|
| ChronicleMap | 0.459 | 0.5 | 0.542 | 0.583 | 0.625 | 0.708 | 0.958 | 5.872 |
| H2 | 1.042 | 1.208 | 1.332 | 1.416 | 1.624 | 1.832 | 2.956 | 8.832 |
| LevelDB | 4.04 | 4.288 | 4.576 | 4.872 | 6.576 | 7.12 | 9.248 | 354.816 |
| MapDB | 5.328 | 5.912 | 6.208 | 6.328 | 6.664 | 7.08 | 8.992 | 19.2 |
| RocksDB | 6.248 | 6.832 | 7.912 | 8.208 | 8.912 | 9.408 | 12 | 21.12 |
