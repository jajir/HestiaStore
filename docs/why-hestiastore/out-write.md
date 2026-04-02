# Benchmark for 'write' operation

## Chart

![Write benchmark chart](../images/out-write.svg)

## Percentile Chart

This chart shows the latency percentile curve for the benchmarked engines. The X axis runs from p50 to p99.99, and the Y axis uses a logarithmic latency scale so tail-latency differences are easier to compare.

![Write latency percentile chart](../images/out-write-percentiles.svg)

## Test Conditions

- Every benchmark in the plain-load suite runs inside the same controlled JVM environment with identical JVM flags and hardware resources. Runs start by wiping the working directory supplied through the `dir` system property, so each trial writes into a fresh, empty location.
- Execution stays single-threaded from warm-up through measurement. The test focuses purely on how quickly one writer can push key/value pairs into the storage engine without any coordination overhead from additional threads.
- Warm-up phases fill the database as aggressively as possible for several 20-second stretches. This stage is meant to trigger JIT compilation, populate caches, and let LevelDB settle into steady-state behaviour before any numbers are recorded.
- Each run exposes the same single-threaded write loop in two JMH modes: `SampleTime` to capture per-operation latency distribution and `Throughput` to capture sustained operations per second.
- Each write operation uses a deterministic pseudo-random long (seed `324432L`) to generate a unique hash string via `HashDataProvider`. The payload is the constant text `"opice skace po stromech"`, so variability comes exclusively from the changing keys.
- After measurements complete, the map is closed and the directory remains available for inspection. The log records how many keys were created, providing a quick sanity check that the run processed the expected volume.
- Test was performed at Mac mini 2024, 16 GB, macOS 15.6.1 (24G90).

## Data for Throughtput Chart

| Engine | Score [ops/s] | Mean [us/op] | p50 [us/op] | p95 [us/op] | p99 [us/op] | Occupied space | CPU Usage |
|:-------|--------------:|-------------:|------------:|------------:|------------:|---------------:|----------:|
| ChronicleMap |         2 216 | 1 056.665 | 2.416 | 826.368 | 1 636.352 | 20.54 GB | 11% |
| H2 |        36 977 | 42.833 | 23.584 | 124.672 | 248.576 | 8 KB | 24% |
| HestiaStoreCompressWrite |        55 268 |  |  |  |  | 2.21 GB | 22% |
| LevelDB |        52 476 | 70.666 | 0.917 | 2.208 | 1 271.808 | 1.31 GB | 12% |
| MapDB |        10 805 | 1 332.197 | 22.272 | 333.824 | 628.736 | 1.91 GB | 5% |
| RocksDB |       217 385 | 20.576 | 2.08 | 3.124 | 4.416 | 2.73 GB | 11% |

## Source Data for Percentile Chart

| Engine | p50 [us/op] | p75 [us/op] | p90 [us/op] | p95 [us/op] | p99 [us/op] | p99.5 [us/op] | p99.9 [us/op] | p99.99 [us/op] |
|:-------|-------------:|-------------:|-------------:|-------------:|-------------:|-------------:|-------------:|-------------:|
| ChronicleMap | 2.416 | 227.072 | 561.152 | 826.368 | 1 636.352 | 3 072 | 30 373.315 | 683 658.969 |
| H2 | 23.584 | 41.024 | 80.384 | 124.672 | 248.576 | 317.952 | 596.992 | 1 896.818 |
| LevelDB | 0.917 | 1.334 | 1.832 | 2.208 | 1 271.808 | 1 304.576 | 1 529.856 | 2 101.246 |
| MapDB | 22.272 | 26.304 | 32.896 | 333.824 | 628.736 | 690.176 | 5 201.92 | 14 060.202 |
| RocksDB | 2.08 | 2.372 | 2.792 | 3.124 | 4.416 | 5.664 | 11.616 | 1 257.472 |
