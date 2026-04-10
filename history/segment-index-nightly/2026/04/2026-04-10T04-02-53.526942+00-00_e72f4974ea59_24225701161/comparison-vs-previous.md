# Benchmark Comparison

- Candidate labels rerun 3x total and median-merged because the initial regression exceeded threshold with high relative score error: `segment-index-get-multisegment-cold,segment-index-mixed-split-heavy`


- Profile: `segment-index-nightly`
- Baseline SHA: `e72f4974ea597aa48d2725b131d13e39bcfa6872`
- Candidate SHA: `e72f4974ea597aa48d2725b131d13e39bcfa6872`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `segment-index-get-multisegment-cold:getHitAsyncJoin` | `50.124 ops/s` | `45.249 ops/s` | `-9.73%` | `worse` |
| `segment-index-get-multisegment-cold:getHitSync` | `41.836 ops/s` | `44.285 ops/s` | `+5.85%` | `better` |
| `segment-index-get-multisegment-cold:getMissAsyncJoin` | `171643.616 ops/s` | `174667.854 ops/s` | `+1.76%` | `neutral` |
| `segment-index-get-multisegment-cold:getMissSync` | `4507423.675 ops/s` | `3905831.604 ops/s` | `-13.35%` | `worse` |
| `segment-index-get-multisegment-hot:getHitAsyncJoin` | `96.289 ops/s` | `97.675 ops/s` | `+1.44%` | `neutral` |
| `segment-index-get-multisegment-hot:getHitSync` | `92.278 ops/s` | `90.199 ops/s` | `-2.25%` | `neutral` |
| `segment-index-get-multisegment-hot:getMissAsyncJoin` | `172310.621 ops/s` | `173282.033 ops/s` | `+0.56%` | `neutral` |
| `segment-index-get-multisegment-hot:getMissSync` | `3753311.780 ops/s` | `4216807.486 ops/s` | `+12.35%` | `better` |
| `segment-index-get-overlay:getHitAsyncJoin` | `164775.014 ops/s` | `166638.793 ops/s` | `+1.13%` | `neutral` |
| `segment-index-get-overlay:getHitSync` | `4070241.270 ops/s` | `3500368.705 ops/s` | `-14.00%` | `worse` |
| `segment-index-get-overlay:getMissAsyncJoin` | `157176.791 ops/s` | `165137.462 ops/s` | `+5.06%` | `better` |
| `segment-index-get-overlay:getMissSync` | `3505853.562 ops/s` | `3868714.651 ops/s` | `+10.35%` | `better` |
| `segment-index-get-persisted:getHitAsyncJoin` | `63957.640 ops/s` | `66348.205 ops/s` | `+3.74%` | `better` |
| `segment-index-get-persisted:getHitSync` | `114137.974 ops/s` | `114860.786 ops/s` | `+0.63%` | `neutral` |
| `segment-index-get-persisted:getMissAsyncJoin` | `164489.667 ops/s` | `165607.589 ops/s` | `+0.68%` | `neutral` |
| `segment-index-get-persisted:getMissSync` | `3581188.277 ops/s` | `4004324.467 ops/s` | `+11.82%` | `better` |
| `segment-index-hot-partition-put:putHotPartition` | `3141985.207 ops/s` | `2981990.603 ops/s` | `-5.09%` | `warning` |
| `segment-index-hot-partition-put:putThenGetHotPartition` | `1715208.301 ops/s` | `1711603.505 ops/s` | `-0.21%` | `neutral` |
| `segment-index-lifecycle:openAndCheckAndRepairConsistency` | `245.014 ms/op` | `241.279 ms/op` | `-1.52%` | `neutral` |
| `segment-index-lifecycle:openAndCompact` | `266.683 ms/op` | `263.803 ms/op` | `-1.08%` | `neutral` |
| `segment-index-lifecycle:openExisting` | `242.128 ms/op` | `239.287 ms/op` | `-1.17%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed` | `535582.626 ops/s` | `531665.950 ops/s` | `-0.73%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed:getWorkload` | `530279.545 ops/s` | `526346.408 ops/s` | `-0.74%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed:putWorkload` | `5303.081 ops/s` | `5319.542 ops/s` | `+0.31%` | `neutral` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed` | `294092.572 ops/s` | `287742.265 ops/s` | `-2.16%` | `neutral` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:getWorkload` | `259843.112 ops/s` | `268288.293 ops/s` | `+3.25%` | `better` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:putWorkload` | `34249.460 ops/s` | `19453.971 ops/s` | `-43.20%` | `worse` |
| `segment-index-persisted-mutation:deleteAsyncJoin` | `1986.574 ops/s` | `2032.617 ops/s` | `+2.32%` | `neutral` |
| `segment-index-persisted-mutation:deleteSync` | `2158.527 ops/s` | `2276.387 ops/s` | `+5.46%` | `better` |
| `segment-index-persisted-mutation:putAsyncJoin` | `1952.973 ops/s` | `2027.844 ops/s` | `+3.83%` | `better` |
| `segment-index-persisted-mutation:putSync` | `2144.307 ops/s` | `2203.288 ops/s` | `+2.75%` | `neutral` |
| `single-chunk-entry-write-compact:writeEntrySteadyState` | `8516521.893 ops/s` | `8464502.980 ops/s` | `-0.61%` | `neutral` |
| `single-chunk-entry-write-large:writeEntrySteadyState` | `7964969.448 ops/s` | `8006915.391 ops/s` | `+0.53%` | `neutral` |
| `sorted-data-diff-key-read-compact:readNextKey` | `9489752.813 ops/s` | `9354328.863 ops/s` | `-1.43%` | `neutral` |
| `sorted-data-diff-key-read-large:readNextKey` | `7367750.596 ops/s` | `7453535.983 ops/s` | `+1.16%` | `neutral` |
