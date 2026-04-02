# Benchmark for 'Multithread Write' operation

## Chart

![Multithread write benchmark chart](../images/out-multithread-write.svg)

## Percentile Chart

This chart shows the latency percentile curve for the benchmarked engines. The X axis runs from p50 to p99.99, and the Y axis uses a logarithmic latency scale so tail-latency differences are easier to compare.

![Multithread write latency percentile chart](../images/out-multithread-write-percentiles.svg)

## Test Conditions - Multithread Write Benchmarks

- Multithread write runs reuse the same controlled JVM flags and hardware as the other benchmark suites. Each trial wipes the working directory supplied through the `dir` system property and creates a fresh storage instance before any benchmark thread starts.
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
| ChronicleMap | 4 | 2 454 | 10% |
| H2 | 4 | 39 736 | 30% |
| HestiaStoreBasic | 4 | 423 154 | 21% |
| LevelDB | 4 | 47 190 | 17% |
| MapDB | 4 | 15 559 | 19% |
| RocksDB | 4 | 108 818 | 15% |

## Source Data for Percentile Chart

| Engine | p50 [us/op] | p75 [us/op] | p90 [us/op] | p95 [us/op] | p99 [us/op] | p99.5 [us/op] | p99.9 [us/op] | p99.99 [us/op] |
|:-------|-------------:|-------------:|-------------:|-------------:|-------------:|-------------:|-------------:|-------------:|
| ChronicleMap | 2.748 | 1 167.36 | 2 330.624 | 3 416.064 | 6 283.264 | 7 995.392 | 296 222.72 | 571 589.578 |
| H2 | 49.856 | 90.24 | 172.8 | 302.08 | 1 527.808 | 1 632.256 | 3 096.576 | 5 152.768 |
| HestiaStoreBasic | 2.708 | 3.5 | 5.456 | 6.832 | 10.08 | 12.704 | 30.496 | 3 627.887 |
| LevelDB | 1.54 | 2.292 | 32.832 | 59.584 | 1 282.048 | 1 505.28 | 1 540.096 | 4 165.632 |
| MapDB | 90.624 | 131.84 | 317.952 | 409.6 | 752.64 | 994.304 | 5 685.248 | 14 811.136 |
| RocksDB | 8.04 | 11.456 | 13.04 | 14.288 | 30.24 | 37.888 | 108.16 | 1 570.816 |
