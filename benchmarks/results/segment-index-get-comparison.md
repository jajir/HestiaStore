# SegmentIndexGetBenchmark Comparison

Command used for both runs:

```sh
java -jar benchmarks/target/benchmarks-0.0.6-SNAPSHOT.jar SegmentIndexGetBenchmark \
  -t 4 -wi 2 -i 4 -f 1 -r 1s -w 1s \
  -p snappy=true -p ioThreads=1 \
  -rf json -rff benchmarks/results/<output>.json
```

Configuration:

- `keyCount=12000`
- `maxKeysInChunk=256`
- `valueLength=64`
- `snappy=true`
- `ioThreads=1`
- `threads=4`

Results:

| Benchmark | Baseline | Direct seekable | Variant A | Baseline -> Direct | Direct -> Variant A |
| --- | ---: | ---: | ---: | ---: | ---: |
| `getHitSync` | `83216.199 ops/s` | `168601.418 ops/s` | `156213.035 ops/s` | `+102.6%` | `-7.3%` |
| `getHitAsyncJoin` | `44405.156 ops/s` | `90428.743 ops/s` | `113548.703 ops/s` | `+103.6%` | `+25.6%` |

Variant A in this comparison means:

- low-level seekable reads stay direct/sync
- public `getAsync/putAsync/deleteAsync` execute the whole sync operation on a fixed `index-worker-*` pool sized by `indexWorkerThreadCount`

Artifacts:

- Baseline JSON: `benchmarks/results/segment-index-get-baseline.json`
- Direct-seekable JSON: `benchmarks/results/segment-index-get-direct-seekable.json`
- Variant A JSON: `benchmarks/results/segment-index-get-variant-a.json`
