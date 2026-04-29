# Benchmark Comparison

- Candidate labels rerun 3x total and median-merged because the initial regression exceeded threshold with high relative score error: `segment-index-get-multisegment-cold`


- Profile: `segment-index-nightly`
- Baseline SHA: `e72f4974ea597aa48d2725b131d13e39bcfa6872`
- Candidate SHA: `e72f4974ea597aa48d2725b131d13e39bcfa6872`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `segment-index-get-multisegment-cold:getHitAsyncJoin` | `47.298 ops/s` | `37.477 ops/s` | `-20.76%` | `worse` |
| `segment-index-get-multisegment-cold:getHitSync` | `44.734 ops/s` | `40.276 ops/s` | `-9.96%` | `worse` |
| `segment-index-get-multisegment-cold:getMissAsyncJoin` | `187147.428 ops/s` | `186157.783 ops/s` | `-0.53%` | `neutral` |
| `segment-index-get-multisegment-cold:getMissSync` | `3359822.060 ops/s` | `3752765.246 ops/s` | `+11.70%` | `better` |
| `segment-index-get-multisegment-hot:getHitAsyncJoin` | `92.960 ops/s` | `104.915 ops/s` | `+12.86%` | `better` |
| `segment-index-get-multisegment-hot:getHitSync` | `89.072 ops/s` | `93.744 ops/s` | `+5.25%` | `better` |
| `segment-index-get-multisegment-hot:getMissAsyncJoin` | `186466.856 ops/s` | `183199.222 ops/s` | `-1.75%` | `neutral` |
| `segment-index-get-multisegment-hot:getMissSync` | `3513302.350 ops/s` | `3662625.777 ops/s` | `+4.25%` | `better` |
| `segment-index-get-overlay:getHitAsyncJoin` | `180260.448 ops/s` | `188661.291 ops/s` | `+4.66%` | `better` |
| `segment-index-get-overlay:getHitSync` | `3727323.153 ops/s` | `3780621.530 ops/s` | `+1.43%` | `neutral` |
| `segment-index-get-overlay:getMissAsyncJoin` | `180728.928 ops/s` | `185714.962 ops/s` | `+2.76%` | `neutral` |
| `segment-index-get-overlay:getMissSync` | `3626607.918 ops/s` | `3440694.772 ops/s` | `-5.13%` | `warning` |
| `segment-index-get-persisted:getHitAsyncJoin` | `66323.493 ops/s` | `63529.366 ops/s` | `-4.21%` | `warning` |
| `segment-index-get-persisted:getHitSync` | `112718.281 ops/s` | `113778.983 ops/s` | `+0.94%` | `neutral` |
| `segment-index-get-persisted:getMissAsyncJoin` | `186291.075 ops/s` | `186403.449 ops/s` | `+0.06%` | `neutral` |
| `segment-index-get-persisted:getMissSync` | `3605450.238 ops/s` | `3842885.793 ops/s` | `+6.59%` | `better` |
| `segment-index-hot-partition-put:putHotPartition` | `2568871.245 ops/s` | `2461260.893 ops/s` | `-4.19%` | `warning` |
| `segment-index-hot-partition-put:putThenGetHotPartition` | `1407159.384 ops/s` | `1540695.145 ops/s` | `+9.49%` | `better` |
| `segment-index-lifecycle:openAndCheckAndRepairConsistency` | `277.869 ms/op` | `276.824 ms/op` | `-0.38%` | `neutral` |
| `segment-index-lifecycle:openAndCompact` | `299.121 ms/op` | `302.007 ms/op` | `+0.96%` | `neutral` |
| `segment-index-lifecycle:openExisting` | `273.855 ms/op` | `273.012 ms/op` | `-0.31%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed` | `511622.560 ops/s` | `508440.161 ops/s` | `-0.62%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed:getWorkload` | `506225.314 ops/s` | `503107.774 ops/s` | `-0.62%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed:putWorkload` | `5397.246 ops/s` | `5332.388 ops/s` | `-1.20%` | `neutral` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed` | `261729.683 ops/s` | `-` | `-` | `removed` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:getWorkload` | `260260.418 ops/s` | `-` | `-` | `removed` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:putWorkload` | `1469.265 ops/s` | `-` | `-` | `removed` |
| `segment-index-persisted-mutation:deleteAsyncJoin` | `2513.217 ops/s` | `2507.743 ops/s` | `-0.22%` | `neutral` |
| `segment-index-persisted-mutation:deleteSync` | `2777.465 ops/s` | `2783.218 ops/s` | `+0.21%` | `neutral` |
| `segment-index-persisted-mutation:putAsyncJoin` | `2468.463 ops/s` | `2497.622 ops/s` | `+1.18%` | `neutral` |
| `segment-index-persisted-mutation:putSync` | `2758.963 ops/s` | `2636.956 ops/s` | `-4.42%` | `warning` |
| `single-chunk-entry-write-compact:writeEntrySteadyState` | `8286342.521 ops/s` | `8232389.306 ops/s` | `-0.65%` | `neutral` |
| `single-chunk-entry-write-large:writeEntrySteadyState` | `7798420.045 ops/s` | `7740914.916 ops/s` | `-0.74%` | `neutral` |
| `sorted-data-diff-key-read-compact:readNextKey` | `8550848.010 ops/s` | `7799903.885 ops/s` | `-8.78%` | `worse` |
| `sorted-data-diff-key-read-large:readNextKey` | `6678947.451 ops/s` | `6305364.295 ops/s` | `-5.59%` | `warning` |
