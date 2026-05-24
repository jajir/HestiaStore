# Benchmark Comparison

- Candidate labels rerun 3x total and median-merged because the initial regression exceeded threshold with high relative score error: `segment-index-get-multisegment-cold,segment-index-get-multisegment-hot,segment-index-lifecycle,segment-index-persisted-mutation`


- Profile: `segment-index-nightly`
- Baseline SHA: `e72f4974ea597aa48d2725b131d13e39bcfa6872`
- Candidate SHA: `e72f4974ea597aa48d2725b131d13e39bcfa6872`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `segment-index-get-multisegment-cold:getHitAsyncJoin` | `46.849 ops/s` | `40.331 ops/s` | `-13.91%` | `worse` |
| `segment-index-get-multisegment-cold:getHitSync` | `40.873 ops/s` | `43.021 ops/s` | `+5.25%` | `better` |
| `segment-index-get-multisegment-cold:getMissAsyncJoin` | `187777.801 ops/s` | `170923.819 ops/s` | `-8.98%` | `worse` |
| `segment-index-get-multisegment-cold:getMissSync` | `3723652.000 ops/s` | `3809286.121 ops/s` | `+2.30%` | `neutral` |
| `segment-index-get-multisegment-hot:getHitAsyncJoin` | `98.955 ops/s` | `93.563 ops/s` | `-5.45%` | `warning` |
| `segment-index-get-multisegment-hot:getHitSync` | `93.858 ops/s` | `84.890 ops/s` | `-9.56%` | `worse` |
| `segment-index-get-multisegment-hot:getMissAsyncJoin` | `188124.509 ops/s` | `172666.909 ops/s` | `-8.22%` | `worse` |
| `segment-index-get-multisegment-hot:getMissSync` | `3783549.573 ops/s` | `3724976.172 ops/s` | `-1.55%` | `neutral` |
| `segment-index-get-overlay:getHitAsyncJoin` | `182119.914 ops/s` | `152210.026 ops/s` | `-16.42%` | `worse` |
| `segment-index-get-overlay:getHitSync` | `3474248.398 ops/s` | `4471980.882 ops/s` | `+28.72%` | `better` |
| `segment-index-get-overlay:getMissAsyncJoin` | `186513.904 ops/s` | `155758.345 ops/s` | `-16.49%` | `worse` |
| `segment-index-get-overlay:getMissSync` | `3614659.006 ops/s` | `4147765.046 ops/s` | `+14.75%` | `better` |
| `segment-index-get-persisted:getHitAsyncJoin` | `61820.529 ops/s` | `60748.831 ops/s` | `-1.73%` | `neutral` |
| `segment-index-get-persisted:getHitSync` | `114937.484 ops/s` | `111800.196 ops/s` | `-2.73%` | `neutral` |
| `segment-index-get-persisted:getMissAsyncJoin` | `182198.502 ops/s` | `164586.446 ops/s` | `-9.67%` | `worse` |
| `segment-index-get-persisted:getMissSync` | `3422769.085 ops/s` | `3789571.520 ops/s` | `+10.72%` | `better` |
| `segment-index-hot-partition-put:putHotPartition` | `2923653.934 ops/s` | `2691324.738 ops/s` | `-7.95%` | `worse` |
| `segment-index-hot-partition-put:putThenGetHotPartition` | `1602974.855 ops/s` | `1742304.287 ops/s` | `+8.69%` | `better` |
| `segment-index-lifecycle:openAndCheckAndRepairConsistency` | `277.298 ms/op` | `250.016 ms/op` | `-9.84%` | `worse` |
| `segment-index-lifecycle:openAndCompact` | `297.932 ms/op` | `270.707 ms/op` | `-9.14%` | `worse` |
| `segment-index-lifecycle:openExisting` | `274.555 ms/op` | `246.623 ms/op` | `-10.17%` | `worse` |
| `segment-index-mixed-drain:partitionedIngestMixed` | `516494.624 ops/s` | `509810.092 ops/s` | `-1.29%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed:getWorkload` | `511083.374 ops/s` | `504467.278 ops/s` | `-1.29%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed:putWorkload` | `5411.251 ops/s` | `5342.815 ops/s` | `-1.26%` | `neutral` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed` | `263479.891 ops/s` | `-` | `-` | `removed` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:getWorkload` | `262065.240 ops/s` | `-` | `-` | `removed` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:putWorkload` | `1414.651 ops/s` | `-` | `-` | `removed` |
| `segment-index-persisted-mutation:deleteAsyncJoin` | `2559.516 ops/s` | `1997.463 ops/s` | `-21.96%` | `worse` |
| `segment-index-persisted-mutation:deleteSync` | `2796.787 ops/s` | `2162.545 ops/s` | `-22.68%` | `worse` |
| `segment-index-persisted-mutation:putAsyncJoin` | `2509.645 ops/s` | `1963.957 ops/s` | `-21.74%` | `worse` |
| `segment-index-persisted-mutation:putSync` | `2669.734 ops/s` | `2155.674 ops/s` | `-19.26%` | `worse` |
| `single-chunk-entry-write-compact:writeEntrySteadyState` | `8281588.335 ops/s` | `8571054.372 ops/s` | `+3.50%` | `better` |
| `single-chunk-entry-write-large:writeEntrySteadyState` | `7535485.853 ops/s` | `7895071.729 ops/s` | `+4.77%` | `better` |
| `sorted-data-diff-key-read-compact:readNextKey` | `8603633.352 ops/s` | `9136413.902 ops/s` | `+6.19%` | `better` |
| `sorted-data-diff-key-read-large:readNextKey` | `6754916.559 ops/s` | `7478295.057 ops/s` | `+10.71%` | `better` |
