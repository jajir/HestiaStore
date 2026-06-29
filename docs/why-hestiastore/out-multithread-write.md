# Benchmark for 'Multi-thread write' operation

## Chart

![Multi-thread write benchmark chart](../images/out-multithread-write.svg)

## Percentile Chart

This chart shows the latency percentile curve for the benchmarked engines. The X axis runs from p50 to p99.99, and the Y axis uses a logarithmic latency scale so tail-latency differences are easier to compare.

![Multi-thread write latency percentile chart](../images/out-multithread-write-percentiles.svg)

## Test Conditions - Multi-thread Write Benchmarks

- Multi-thread write runs reuse the same controlled JVM flags and hardware as the other benchmark suites. Each trial wipes the working directory supplied through the `dir` system property and creates a fresh storage instance before any benchmark thread starts.
- Each benchmark thread performs the same write operation in two JMH modes during the same run: `SampleTime` to capture latency percentiles and `Throughput` to capture aggregate write throughput.
- The configured thread count for this result set is 4 benchmark threads, matching the `threads4` suffix used by the generated result files.
- Every operation generates a pseudo-random key via `HashDataProvider.makeHash(ThreadLocalRandom.current().nextLong())`, so concurrent writers insert independent keys while using the constant payload `"opice skace po stromech"`.
- Warm-up uses 10 iterations of 20 seconds, followed by 25 measurement iterations of 20 seconds, so the results represent sustained concurrent write pressure rather than startup behavior.
- The benchmark focuses on contention and latency under concurrent insert load. There is no preload phase for this suite; the store starts empty at the beginning of each trial.
- After measurements complete, the storage is closed and the resulting directory remains available so the reporting scripts can capture occupied space and CPU usage.
- Test was performed at Mac mini 2024, 16 GB, macOS 15.6.1 (24G90).

## Data for Throughtput Chart

| Engine | Threads | Throughput [ops/s] | CPU Usage |
|:-------|--------:|-------------------:|----------:|
| ChronicleMap | 4 | 4 323 | 19% |
| H2 | 4 | 52 951 | 29% |
| HestiaStoreBasic | 4 | 259 048 | 22% |
| HestiaStoreCompress | 4 | 184 416 | 13% |
| LevelDB | 4 | 48 452 | 19% |
| MapDB | 4 | 15 426 | 16% |
| RocksDB | 4 | 145 239 | 18% |

## Source Data for Percentile Chart

| Engine | p50 [us/op] | p75 [us/op] | p90 [us/op] | p95 [us/op] | p99 [us/op] | p99.5 [us/op] | p99.9 [us/op] | p99.99 [us/op] |
|:-------|-------------:|-------------:|-------------:|-------------:|-------------:|-------------:|-------------:|-------------:|
| ChronicleMap | 0.791 | 1 010.688 | 1 712.128 | 2 101.248 | 3 452.928 | 4 136.96 | 6 332.416 | 598 054.483 |
| H2 | 19.744 | 42.816 | 89.6 | 145.664 | 1 531.904 | 1 611.776 | 3 125.248 | 5 718.016 |
| HestiaStoreBasic | 1.5 | 2.5 | 4.832 | 8.832 | 29.536 | 53.312 | 282.112 | 3 110.531 |
| HestiaStoreCompress | 1.458 | 2 | 3 | 4.208 | 16.416 | 26.976 | 150.784 | 2 084.609 |
| LevelDB | 1.584 | 2.332 | 33.792 | 57.216 | 1 513.472 | 1 525.76 | 1 558.528 | 4 186.112 |
| MapDB | 105.6 | 144.384 | 194.816 | 234.496 | 350.208 | 591.872 | 5 660.672 | 12 550.144 |
| RocksDB | 10.368 | 11.616 | 12.656 | 13.696 | 21.984 | 27.552 | 49.728 | 2 132.969 |
