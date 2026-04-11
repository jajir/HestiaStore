# Benchmark Comparison

- Candidate labels rerun 3x total and median-merged because the initial regression exceeded threshold with high relative score error: `segment-index-get-multisegment-cold,segment-index-get-multisegment-hot,segment-index-mixed-split-heavy`


- Profile: `segment-index-nightly`
- Baseline SHA: `e72f4974ea597aa48d2725b131d13e39bcfa6872`
- Candidate SHA: `e72f4974ea597aa48d2725b131d13e39bcfa6872`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `segment-index-get-multisegment-cold:getHitAsyncJoin` | `45.249 ops/s` | `52.910 ops/s` | `+16.93%` | `better` |
| `segment-index-get-multisegment-cold:getHitSync` | `44.285 ops/s` | `44.302 ops/s` | `+0.04%` | `neutral` |
| `segment-index-get-multisegment-cold:getMissAsyncJoin` | `174667.854 ops/s` | `172366.675 ops/s` | `-1.32%` | `neutral` |
| `segment-index-get-multisegment-cold:getMissSync` | `3905831.604 ops/s` | `3608280.445 ops/s` | `-7.62%` | `worse` |
| `segment-index-get-multisegment-hot:getHitAsyncJoin` | `97.675 ops/s` | `89.482 ops/s` | `-8.39%` | `worse` |
| `segment-index-get-multisegment-hot:getHitSync` | `90.199 ops/s` | `85.621 ops/s` | `-5.08%` | `warning` |
| `segment-index-get-multisegment-hot:getMissAsyncJoin` | `173282.033 ops/s` | `171596.313 ops/s` | `-0.97%` | `neutral` |
| `segment-index-get-multisegment-hot:getMissSync` | `4216807.486 ops/s` | `3759477.487 ops/s` | `-10.85%` | `worse` |
| `segment-index-get-overlay:getHitAsyncJoin` | `166638.793 ops/s` | `164945.393 ops/s` | `-1.02%` | `neutral` |
| `segment-index-get-overlay:getHitSync` | `3500368.705 ops/s` | `4077630.974 ops/s` | `+16.49%` | `better` |
| `segment-index-get-overlay:getMissAsyncJoin` | `165137.462 ops/s` | `158126.438 ops/s` | `-4.25%` | `warning` |
| `segment-index-get-overlay:getMissSync` | `3868714.651 ops/s` | `3638837.567 ops/s` | `-5.94%` | `warning` |
| `segment-index-get-persisted:getHitAsyncJoin` | `66348.205 ops/s` | `62744.968 ops/s` | `-5.43%` | `warning` |
| `segment-index-get-persisted:getHitSync` | `114860.786 ops/s` | `115378.181 ops/s` | `+0.45%` | `neutral` |
| `segment-index-get-persisted:getMissAsyncJoin` | `165607.589 ops/s` | `160445.796 ops/s` | `-3.12%` | `warning` |
| `segment-index-get-persisted:getMissSync` | `4004324.467 ops/s` | `3972340.485 ops/s` | `-0.80%` | `neutral` |
| `segment-index-hot-partition-put:putHotPartition` | `2981990.603 ops/s` | `2999846.533 ops/s` | `+0.60%` | `neutral` |
| `segment-index-hot-partition-put:putThenGetHotPartition` | `1711603.505 ops/s` | `1656008.332 ops/s` | `-3.25%` | `warning` |
| `segment-index-lifecycle:openAndCheckAndRepairConsistency` | `241.279 ms/op` | `244.573 ms/op` | `+1.37%` | `neutral` |
| `segment-index-lifecycle:openAndCompact` | `263.803 ms/op` | `266.457 ms/op` | `+1.01%` | `neutral` |
| `segment-index-lifecycle:openExisting` | `239.287 ms/op` | `241.544 ms/op` | `+0.94%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed` | `531665.950 ops/s` | `504348.445 ops/s` | `-5.14%` | `warning` |
| `segment-index-mixed-drain:partitionedIngestMixed:getWorkload` | `526346.408 ops/s` | `499015.290 ops/s` | `-5.19%` | `warning` |
| `segment-index-mixed-drain:partitionedIngestMixed:putWorkload` | `5319.542 ops/s` | `5333.155 ops/s` | `+0.26%` | `neutral` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed` | `287742.265 ops/s` | `266790.825 ops/s` | `-7.28%` | `worse` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:getWorkload` | `268288.293 ops/s` | `265374.657 ops/s` | `-1.09%` | `neutral` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:putWorkload` | `19453.971 ops/s` | `1512.868 ops/s` | `-92.22%` | `worse` |
| `segment-index-persisted-mutation:deleteAsyncJoin` | `2032.617 ops/s` | `2021.502 ops/s` | `-0.55%` | `neutral` |
| `segment-index-persisted-mutation:deleteSync` | `2276.387 ops/s` | `2232.959 ops/s` | `-1.91%` | `neutral` |
| `segment-index-persisted-mutation:putAsyncJoin` | `2027.844 ops/s` | `2001.895 ops/s` | `-1.28%` | `neutral` |
| `segment-index-persisted-mutation:putSync` | `2203.288 ops/s` | `2147.591 ops/s` | `-2.53%` | `neutral` |
| `single-chunk-entry-write-compact:writeEntrySteadyState` | `8464502.980 ops/s` | `8516272.167 ops/s` | `+0.61%` | `neutral` |
| `single-chunk-entry-write-large:writeEntrySteadyState` | `8006915.391 ops/s` | `7925505.610 ops/s` | `-1.02%` | `neutral` |
| `sorted-data-diff-key-read-compact:readNextKey` | `9354328.863 ops/s` | `9455176.095 ops/s` | `+1.08%` | `neutral` |
| `sorted-data-diff-key-read-large:readNextKey` | `7453535.983 ops/s` | `7455649.855 ops/s` | `+0.03%` | `neutral` |
