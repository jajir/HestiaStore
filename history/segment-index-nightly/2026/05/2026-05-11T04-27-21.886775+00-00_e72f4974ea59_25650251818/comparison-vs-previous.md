# Benchmark Comparison

- Candidate labels rerun 3x total and median-merged because the initial regression exceeded threshold with high relative score error: `segment-index-get-multisegment-hot,segment-index-lifecycle`


- Profile: `segment-index-nightly`
- Baseline SHA: `e72f4974ea597aa48d2725b131d13e39bcfa6872`
- Candidate SHA: `e72f4974ea597aa48d2725b131d13e39bcfa6872`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `segment-index-get-multisegment-cold:getHitAsyncJoin` | `48.176 ops/s` | `46.020 ops/s` | `-4.48%` | `warning` |
| `segment-index-get-multisegment-cold:getHitSync` | `44.551 ops/s` | `48.534 ops/s` | `+8.94%` | `better` |
| `segment-index-get-multisegment-cold:getMissAsyncJoin` | `188854.683 ops/s` | `170559.273 ops/s` | `-9.69%` | `worse` |
| `segment-index-get-multisegment-cold:getMissSync` | `3838266.355 ops/s` | `3409275.893 ops/s` | `-11.18%` | `worse` |
| `segment-index-get-multisegment-hot:getHitAsyncJoin` | `102.753 ops/s` | `85.659 ops/s` | `-16.64%` | `worse` |
| `segment-index-get-multisegment-hot:getHitSync` | `83.125 ops/s` | `85.432 ops/s` | `+2.77%` | `neutral` |
| `segment-index-get-multisegment-hot:getMissAsyncJoin` | `188640.493 ops/s` | `171762.257 ops/s` | `-8.95%` | `worse` |
| `segment-index-get-multisegment-hot:getMissSync` | `3482966.464 ops/s` | `3702433.533 ops/s` | `+6.30%` | `better` |
| `segment-index-get-overlay:getHitAsyncJoin` | `187977.994 ops/s` | `160906.402 ops/s` | `-14.40%` | `worse` |
| `segment-index-get-overlay:getHitSync` | `3933355.351 ops/s` | `4316645.484 ops/s` | `+9.74%` | `better` |
| `segment-index-get-overlay:getMissAsyncJoin` | `183338.261 ops/s` | `164524.113 ops/s` | `-10.26%` | `worse` |
| `segment-index-get-overlay:getMissSync` | `3591921.901 ops/s` | `3876995.722 ops/s` | `+7.94%` | `better` |
| `segment-index-get-persisted:getHitAsyncJoin` | `68169.104 ops/s` | `61040.115 ops/s` | `-10.46%` | `worse` |
| `segment-index-get-persisted:getHitSync` | `115919.438 ops/s` | `117449.237 ops/s` | `+1.32%` | `neutral` |
| `segment-index-get-persisted:getMissAsyncJoin` | `188831.688 ops/s` | `165043.257 ops/s` | `-12.60%` | `worse` |
| `segment-index-get-persisted:getMissSync` | `3719862.867 ops/s` | `3644350.438 ops/s` | `-2.03%` | `neutral` |
| `segment-index-hot-partition-put:putHotPartition` | `2612360.300 ops/s` | `2878623.005 ops/s` | `+10.19%` | `better` |
| `segment-index-hot-partition-put:putThenGetHotPartition` | `1594268.754 ops/s` | `1651247.918 ops/s` | `+3.57%` | `better` |
| `segment-index-lifecycle:openAndCheckAndRepairConsistency` | `279.334 ms/op` | `247.774 ms/op` | `-11.30%` | `worse` |
| `segment-index-lifecycle:openAndCompact` | `305.404 ms/op` | `268.843 ms/op` | `-11.97%` | `worse` |
| `segment-index-lifecycle:openExisting` | `278.137 ms/op` | `245.339 ms/op` | `-11.79%` | `worse` |
| `segment-index-mixed-drain:partitionedIngestMixed` | `517884.680 ops/s` | `505009.501 ops/s` | `-2.49%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed:getWorkload` | `512538.252 ops/s` | `499740.809 ops/s` | `-2.50%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed:putWorkload` | `5346.428 ops/s` | `5268.692 ops/s` | `-1.45%` | `neutral` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed` | `266275.783 ops/s` | `260190.301 ops/s` | `-2.29%` | `neutral` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:getWorkload` | `264778.926 ops/s` | `258846.322 ops/s` | `-2.24%` | `neutral` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:putWorkload` | `1496.857 ops/s` | `1343.979 ops/s` | `-10.21%` | `worse` |
| `segment-index-persisted-mutation:deleteAsyncJoin` | `2522.792 ops/s` | `1966.338 ops/s` | `-22.06%` | `worse` |
| `segment-index-persisted-mutation:deleteSync` | `2752.565 ops/s` | `2110.536 ops/s` | `-23.32%` | `worse` |
| `segment-index-persisted-mutation:putAsyncJoin` | `2472.437 ops/s` | `1979.715 ops/s` | `-19.93%` | `worse` |
| `segment-index-persisted-mutation:putSync` | `2724.260 ops/s` | `2110.956 ops/s` | `-22.51%` | `worse` |
| `single-chunk-entry-write-compact:writeEntrySteadyState` | `8313735.079 ops/s` | `8457547.645 ops/s` | `+1.73%` | `neutral` |
| `single-chunk-entry-write-large:writeEntrySteadyState` | `7840551.997 ops/s` | `7911274.451 ops/s` | `+0.90%` | `neutral` |
| `sorted-data-diff-key-read-compact:readNextKey` | `8269018.385 ops/s` | `9141101.338 ops/s` | `+10.55%` | `better` |
| `sorted-data-diff-key-read-large:readNextKey` | `6771093.069 ops/s` | `7068451.107 ops/s` | `+4.39%` | `better` |
