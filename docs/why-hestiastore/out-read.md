# Benchmark for 'read' operation

## Chart

![Read benchmark chart](../images/out-read.svg)

## Percentile Chart

This chart shows the latency percentile curve for the benchmarked engines. The X axis runs from p50 to p99.99, and the Y axis uses a logarithmic latency scale so tail-latency differences are easier to compare.

![Read latency percentile chart](../images/out-read-percentiles.svg)

## Test Conditions - Read Benchmarks

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
| ChronicleMap |     2 098 866 | 0.49 | 0.5 | 0.625 | 0.792 | 2.03 GB | 8% |
| H2 |       913 355 | 1.11 | 1.082 | 1.5 | 1.874 | 8 KB | 9% |
| HestiaStoreBasic |         6 589 | 155.204 | 196.864 | 247.808 | 469.504 | 542.14 MB | 11% |
| HestiaStoreCompress |         6 976 | 146.445 | 198.912 | 229.888 | 381.952 | 328.7 MB | 12% |
| LevelDB |       245 945 | 4.739 | 4.208 | 7.496 | 8.864 | 363.23 MB | 8% |
| MapDB |       187 277 | 6.976 | 5.416 | 6.744 | 8.288 | 1.3 GB | 6% |
| RocksDB |       151 412 | 6.746 | 6.328 | 8.368 | 9.696 | 324.23 MB | 9% |

## Source Data for Percentile Chart

| Engine | p50 [us/op] | p75 [us/op] | p90 [us/op] | p95 [us/op] | p99 [us/op] | p99.5 [us/op] | p99.9 [us/op] | p99.99 [us/op] |
|:-------|-------------:|-------------:|-------------:|-------------:|-------------:|-------------:|-------------:|-------------:|
| ChronicleMap | 0.5 | 0.541 | 0.583 | 0.625 | 0.792 | 0.958 | 1.458 | 6.784 |
| H2 | 1.082 | 1.25 | 1.374 | 1.5 | 1.874 | 2.5 | 3.664 | 11.616 |
| HestiaStoreBasic | 196.864 | 216.576 | 233.216 | 247.808 | 469.504 | 494.592 | 567.296 | 916.398 |
| HestiaStoreCompress | 198.912 | 209.664 | 219.904 | 229.888 | 381.952 | 395.776 | 539.648 | 1 250.428 |
| LevelDB | 4.208 | 5.576 | 6.536 | 7.496 | 8.864 | 9.872 | 13.04 | 384.512 |
| MapDB | 5.416 | 5.952 | 6.496 | 6.744 | 8.288 | 10 | 351.744 | 643.072 |
| RocksDB | 6.328 | 7 | 8.04 | 8.368 | 9.696 | 11.12 | 14.528 | 33.856 |
