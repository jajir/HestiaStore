# SegmentIndex Direct-to-Segment Smoke

Quick smoke results for the direct-to-segment benchmark additions.

These are not stable baselines. They were collected with a single short JMH
measurement iteration to validate that the new benchmark scenarios run end to
end after the direct-to-segment routing refactor.

## Commands

```sh
java -jar benchmarks/target/benchmarks-0.0.6-SNAPSHOT.jar SegmentIndexGetBenchmark \
  -p readPathMode=live -p snappy=false -p keyCount=2048 -p maxKeysInChunk=256 \
  -p valueLength=32 -wi 0 -i 1 -f 1 -r 1s -w 1s

java -jar benchmarks/target/benchmarks-0.0.6-SNAPSHOT.jar SegmentIndexMixedDrainBenchmark \
  -p workloadMode=drainOnly -wi 0 -i 1 -f 1 -r 1s -w 1s

java -jar benchmarks/target/benchmarks-0.0.6-SNAPSHOT.jar SegmentIndexMixedDrainBenchmark \
  -p workloadMode=splitHeavy -wi 0 -i 1 -f 1 -r 1s -w 1s
```

## Observed Smoke Results

| Benchmark | Mode | Score |
| --- | --- | ---: |
| `SegmentIndexGetBenchmark.getHitSync` | live | `9_538_398.563 ops/s` |
| `SegmentIndexGetBenchmark.getHitAsyncJoin` | live | `231_584.445 ops/s` |
| `SegmentIndexMixedDrainBenchmark.routedMixed:getWorkload` | drainOnly | `477_784.347 ops/s` |
| `SegmentIndexMixedDrainBenchmark.routedMixed:putWorkload` | drainOnly | `5_417.866 ops/s` |
| `SegmentIndexMixedDrainBenchmark.routedMixed:getWorkload` | splitHeavy | `130_136.021 ops/s` |
| `SegmentIndexMixedDrainBenchmark.routedMixed:putWorkload` | splitHeavy | `4_833.321 ops/s` |

## Notes

- `SegmentIndexGetBenchmark` now measures both persisted-only and live-update
  lookup paths through the `readPathMode` parameter.
- `SegmentIndexMixedDrainBenchmark` now supports:
  - `workloadMode=drainOnly` for one hot routed range under continuous
    maintenance pressure
  - `workloadMode=splitHeavy` for a growing routed keyspace that keeps background split policy active
- The JMH group uses 16 writer threads and 4 reader threads.
- For comparable before/after performance analysis, rerun with longer warm-up,
  multiple forks, fixed profiler settings, and archived JSON outputs.
