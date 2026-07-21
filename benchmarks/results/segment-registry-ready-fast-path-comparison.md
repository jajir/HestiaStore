# Segment Registry READY Fast-Path Comparison

Comparison date: 2026-07-21

Environment:

- JMH 1.37
- OpenJDK 25.0.3
- Four benchmark threads
- Live read path with the chunk-store page cache disabled

## Throughput command

```sh
java -jar benchmarks/target/benchmarks-1.0.1-SNAPSHOT.jar \
  'SegmentIndexGetBenchmark.getHitSync' \
  -t 4 -wi 3 -i 5 -f 3 -r 1s -w 1s \
  -p snappy=true -p ioThreads=1 \
  -p readPathMode=live -p chunkStoreCachePageLimit=0 \
  -rf json -rff <output>.json
```

## Results

| Measurement | Before | After | Change |
| --- | ---: | ---: | ---: |
| Mean throughput | 2,625,001 ops/s | 3,410,561 ops/s | +29.9% |
| Sample median | 2,861,384 ops/s | 3,559,832 ops/s | +24.4% |
| JMH error (99.9%) | 534,575 ops/s | 792,803 ops/s | — |

The throughput signal is positive, but the 99.9% confidence intervals overlap
because the short local runs were noisy. The result should therefore be treated
as directional rather than a precise expected gain.

Raw JMH output:

- `segment-registry-ready-fast-path-before.json`
- `segment-registry-ready-fast-path-after.json`

## Stack-profile comparison

Both profiles used two one-second warmups, four one-second measurements, one
fork, and a five-millisecond sampling period.

| Thread-state sample | Before | After |
| --- | ---: | ---: |
| Raw `WAITING` samples | 27.8% | 3.6% |
| Initial registry-load entry lock | 50.8% of `WAITING` | 0% |
| Blocking-wrapper second-load entry lock | 36.5% of `WAITING` | 0% |
| Raw `BLOCKED` samples | 1.0% | 10.6% |

The two targeted `SegmentRegistryEntry.waitWhileLoading()` contention paths
were removed. The post-change profile instead identifies
`SessionOperationGate` monitor entry/exit as the next synchronization bottleneck.
