# Benchmark for 'Sequential read' operation

## Chart

![Sequential read benchmark chart](../images/out-sequential.svg)

## Percentile Chart

This chart shows the latency percentile curve for the benchmarked engines. The X axis runs from p50 to p99.99, and the Y axis uses a logarithmic latency scale so tail-latency differences are easier to compare.

![Sequential read latency percentile chart](../images/out-sequential-percentiles.svg)

## Test Conditions - Sequential Read Benchmarks

- Each sequential scenario uses the same JVM flags, hardware, and scratch directory handling as the write/read suites. The `dir` property is cleaned before every run to guarantee a fresh start.
- Setup writes 10 000 000 deterministic key/value pairs (seed `324432L`) into the engine. Keys are generated via `HashDataProvider` so that the exact ordering is reproducible across runs.
- After preloading, the benchmark resets its sequential cursor. Warm-up iterations walk the keyspace from the first key to the last key so caches and OS I/O buffers reflect streaming access.
- Each run exposes the same single-threaded sequential scan in two JMH modes: `SampleTime` to capture per-operation latency distribution and `Throughput` to capture sustained ordered-read performance.
- The read workload remains single-threaded; each invocation issues exactly one lookup to keep measurements comparable with the other suites.
- Directories remain on disk after the run so disk usage and auxiliary metrics can be collected by reporting scripts.
- Tests for HestiaStoreStream use dedicated stream API. Without using Stream API is performance visible in line HestiaStoreBasic.
- Tests executed on Mac mini 2024, 16 GB RAM, macOS 15.6.1 (24G90).

## Data for Throughtput Chart

| Engine | Score [ops/s] | Mean [us/op] | p50 [us/op] | p95 [us/op] | p99 [us/op] | Occupied space | CPU Usage |
|:-------|--------------:|-------------:|------------:|------------:|------------:|---------------:|----------:|
| ChronicleMap |     2 420 667 | 0.43 | 0.417 | 0.541 | 0.625 | 2.03 GB | 8% |
| H2 |       961 605 | 1.075 | 1.042 | 1.458 | 1.666 | 8 KB | 8% |
| HestiaStoreBasic |         5 308 | 186.856 | 215.04 | 242.176 | 508.928 | 1.41 GB | 10% |
| HestiaStoreCompress |         5 607 | 177.582 | 212.48 | 227.84 | 384 | 862.85 MB | 11% |
| HestiaStoreStream |    10 386 149 | 0.173 | 0.042 | 0.125 | 0.209 | 842.85 MB | 8% |
| LevelDB |       116 453 | 8.589 | 7.744 | 11.536 | 13.408 | 363.44 MB | 9% |
| MapDB |       190 100 | 5.476 | 5.288 | 6.288 | 6.624 | 1.3 GB | 6% |
| RocksDB |       167 813 | 5.968 | 6.08 | 6.912 | 7.744 | 324.23 MB | 8% |

## Source Data for Percentile Chart

| Engine | p50 [us/op] | p75 [us/op] | p90 [us/op] | p95 [us/op] | p99 [us/op] | p99.5 [us/op] | p99.9 [us/op] | p99.99 [us/op] |
|:-------|-------------:|-------------:|-------------:|-------------:|-------------:|-------------:|-------------:|-------------:|
| ChronicleMap | 0.417 | 0.459 | 0.5 | 0.541 | 0.625 | 0.666 | 0.875 | 5.416 |
| H2 | 1.042 | 1.208 | 1.334 | 1.458 | 1.666 | 1.834 | 3 | 9.04 |
| HestiaStoreBasic | 215.04 | 219.904 | 226.304 | 242.176 | 508.928 | 514.048 | 572.416 | 23 822.336 |
| HestiaStoreCompress | 212.48 | 215.552 | 220.416 | 227.84 | 384 | 388.608 | 451.584 | 23 691.264 |
| HestiaStoreStream | 0.042 | 0.042 | 0.084 | 0.125 | 0.209 | 0.25 | 1.624 | 17.824 |
| LevelDB | 7.744 | 9.952 | 10.944 | 11.536 | 13.408 | 14.528 | 18.784 | 380.416 |
| MapDB | 5.288 | 5.912 | 6.16 | 6.288 | 6.624 | 7.16 | 9.072 | 22.528 |
| RocksDB | 6.08 | 6.368 | 6.704 | 6.912 | 7.744 | 8.528 | 10.528 | 20.096 |
