# Benchmark for 'Single-thread write' operation

## Chart

![Single-thread write benchmark chart](../images/out-write.svg)

## Percentile Chart

This chart shows the latency percentile curve for the benchmarked engines. The X axis runs from p50 to p99.99, and the Y axis uses a logarithmic latency scale so tail-latency differences are easier to compare.

![Single-thread write latency percentile chart](../images/out-write-percentiles.svg)

## Test Conditions

- Every benchmark in the single-thread write suite runs inside the same controlled JVM environment with identical JVM flags and hardware resources. Runs start by wiping the working directory supplied through the `dir` system property, so each trial writes into a fresh, empty location.
- Execution stays single-threaded from warm-up through measurement. The test focuses purely on how quickly one writer can push key/value pairs into the storage engine without any coordination overhead from additional threads.
- Warm-up phases fill the database as aggressively as possible for several 20-second stretches. This stage is meant to trigger JIT compilation, populate caches, and let LevelDB settle into steady-state behaviour before any numbers are recorded.
- Each run exposes the same single-threaded write loop in two JMH modes: `SampleTime` to capture per-operation latency distribution and `Throughput` to capture sustained operations per second.
- Each write operation uses a deterministic pseudo-random long (seed `324432L`) to generate a unique hash string via `HashDataProvider`. The payload is the constant text `"opice skace po stromech"`, so variability comes exclusively from the changing keys.
- After measurements complete, the map is closed and the directory remains available for inspection. The log records how many keys were created, providing a quick sanity check that the run processed the expected volume.
- Test was performed at Mac mini 2024, 16 GB, macOS 15.6.1 (24G90).

## Data for Throughtput Chart

| Engine | Score [ops/s] | Mean [us/op] | p50 [us/op] | p95 [us/op] | p99 [us/op] | Occupied space | CPU Usage |
|:-------|--------------:|-------------:|------------:|------------:|------------:|---------------:|----------:|
| ChronicleMap |         3 892 | 207.376 | 0.791 | 529.408 | 692.224 | 20.54 GB | 14% |
| H2 |        36 074 | 24.109 | 13.408 | 69.12 | 158.464 | 8 KB | 21% |
| LevelDB |        48 597 | 25.587 | 0.875 | 2.124 | 1 505.28 | 1.56 GB | 13% |
| MapDB |        11 098 | 120.851 | 22.816 | 33.984 | 104.448 | 2.32 GB | 12% |
| RocksDB |       210 418 | 18.484 | 2.08 | 3.164 | 5.16 | 4.91 GB | 12% |

## Source Data for Percentile Chart

| Engine | p50 [us/op] | p75 [us/op] | p90 [us/op] | p95 [us/op] | p99 [us/op] | p99.5 [us/op] | p99.9 [us/op] | p99.99 [us/op] |
|:-------|-------------:|-------------:|-------------:|-------------:|-------------:|-------------:|-------------:|-------------:|
| ChronicleMap | 0.791 | 280.576 | 435.712 | 529.408 | 692.224 | 884.736 | 1 286.144 | 68 498.437 |
| H2 | 13.408 | 24.032 | 43.392 | 69.12 | 158.464 | 211.2 | 403.617 | 1 220.608 |
| LevelDB | 0.875 | 1.292 | 1.75 | 2.124 | 1 505.28 | 1 523.712 | 1 531.904 | 2 240.512 |
| MapDB | 22.816 | 26.24 | 30.24 | 33.984 | 104.448 | 1 763.328 | 2 961.408 | 4 993.712 |
| RocksDB | 2.08 | 2.372 | 2.792 | 3.164 | 5.16 | 8.32 | 24.992 | 1 527.808 |
