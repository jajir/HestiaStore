# Unique Cache Sorted-Key Snapshot Comparison

Comparison date: 2026-07-29

Environment:

- JMH 1.37
- Eclipse Temurin 17.0.19
- One benchmark thread
- Three independent JVM forks
- Three one-second warmups and five one-second measurements per fork
- GC allocation profiler enabled

The fixture inserts deterministic, distinct, mixed `Integer` keys. Trial setup
also compares every key returned by the legacy and production iterators before
measurement, including order and total count.

## Command

```sh
java -jar benchmarks/target/benchmarks-1.0.1-SNAPSHOT.jar \
  'UniqueCacheSortedKeyIteratorBenchmark' \
  -wi 3 -i 5 -f 3 -w 1s -r 1s -prof gc \
  -rf json -rff <output>.json
```

## Results

| Keys | Measurement | `ArrayList` snapshot | Reused array | Observed change |
| ---: | --- | ---: | ---: | ---: |
| 100,000 | Allocation | 1,261,742 B/op | 861,717 B/op | -31.7% |
| 500,000 | Allocation | 6,048,668 B/op | 4,048,667 B/op | -33.1% |
| 100,000 | Mean time | 10.533 ± 0.184 ms/op | 10.472 ± 0.245 ms/op | -0.6% |
| 500,000 | Mean time | 55.352 ± 0.509 ms/op | 58.943 ± 5.175 ms/op | +6.5% |

Errors are JMH 99.9% confidence intervals.

The allocation result is conclusive: reusing the key-set array removed about
400 KB per 100,000 keys and 2.0 MB per 500,000 keys. The remaining allocation
includes the key-set snapshot and the sorting algorithm's temporary storage.

The timing result does not demonstrate a speedup or a regression. The
100,000-key means are effectively equal, while the noisy 500,000-key reused
array result has a confidence interval of 53.768–64.118 ms/op, overlapping the
legacy interval of 54.843–55.861 ms/op. The justified benefit from this
measurement is therefore lower temporary allocation and GC pressure, not faster
sorting.
