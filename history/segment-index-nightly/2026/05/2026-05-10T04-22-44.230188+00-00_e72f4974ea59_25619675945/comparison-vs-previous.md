# Benchmark Comparison

- Candidate labels rerun 3x total and median-merged because the initial regression exceeded threshold with high relative score error: `segment-index-get-multisegment-cold`


- Profile: `segment-index-nightly`
- Baseline SHA: `e72f4974ea597aa48d2725b131d13e39bcfa6872`
- Candidate SHA: `e72f4974ea597aa48d2725b131d13e39bcfa6872`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `segment-index-get-multisegment-cold:getHitAsyncJoin` | `44.588 ops/s` | `48.176 ops/s` | `+8.05%` | `better` |
| `segment-index-get-multisegment-cold:getHitSync` | `53.367 ops/s` | `44.551 ops/s` | `-16.52%` | `worse` |
| `segment-index-get-multisegment-cold:getMissAsyncJoin` | `172199.675 ops/s` | `188854.683 ops/s` | `+9.67%` | `better` |
| `segment-index-get-multisegment-cold:getMissSync` | `4001960.958 ops/s` | `3838266.355 ops/s` | `-4.09%` | `warning` |
| `segment-index-get-multisegment-hot:getHitAsyncJoin` | `94.963 ops/s` | `102.753 ops/s` | `+8.20%` | `better` |
| `segment-index-get-multisegment-hot:getHitSync` | `82.793 ops/s` | `83.125 ops/s` | `+0.40%` | `neutral` |
| `segment-index-get-multisegment-hot:getMissAsyncJoin` | `171754.618 ops/s` | `188640.493 ops/s` | `+9.83%` | `better` |
| `segment-index-get-multisegment-hot:getMissSync` | `3469153.716 ops/s` | `3482966.464 ops/s` | `+0.40%` | `neutral` |
| `segment-index-get-overlay:getHitAsyncJoin` | `165085.195 ops/s` | `187977.994 ops/s` | `+13.87%` | `better` |
| `segment-index-get-overlay:getHitSync` | `3767196.717 ops/s` | `3933355.351 ops/s` | `+4.41%` | `better` |
| `segment-index-get-overlay:getMissAsyncJoin` | `164325.883 ops/s` | `183338.261 ops/s` | `+11.57%` | `better` |
| `segment-index-get-overlay:getMissSync` | `3552788.672 ops/s` | `3591921.901 ops/s` | `+1.10%` | `neutral` |
| `segment-index-get-persisted:getHitAsyncJoin` | `60574.799 ops/s` | `68169.104 ops/s` | `+12.54%` | `better` |
| `segment-index-get-persisted:getHitSync` | `113327.836 ops/s` | `115919.438 ops/s` | `+2.29%` | `neutral` |
| `segment-index-get-persisted:getMissAsyncJoin` | `164442.723 ops/s` | `188831.688 ops/s` | `+14.83%` | `better` |
| `segment-index-get-persisted:getMissSync` | `4152657.488 ops/s` | `3719862.867 ops/s` | `-10.42%` | `worse` |
| `segment-index-hot-partition-put:putHotPartition` | `3047695.327 ops/s` | `2612360.300 ops/s` | `-14.28%` | `worse` |
| `segment-index-hot-partition-put:putThenGetHotPartition` | `1555331.610 ops/s` | `1594268.754 ops/s` | `+2.50%` | `neutral` |
| `segment-index-lifecycle:openAndCheckAndRepairConsistency` | `242.150 ms/op` | `279.334 ms/op` | `+15.36%` | `better` |
| `segment-index-lifecycle:openAndCompact` | `266.261 ms/op` | `305.404 ms/op` | `+14.70%` | `better` |
| `segment-index-lifecycle:openExisting` | `241.193 ms/op` | `278.137 ms/op` | `+15.32%` | `better` |
| `segment-index-mixed-drain:partitionedIngestMixed` | `516968.533 ops/s` | `517884.680 ops/s` | `+0.18%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed:getWorkload` | `511661.230 ops/s` | `512538.252 ops/s` | `+0.17%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed:putWorkload` | `5307.303 ops/s` | `5346.428 ops/s` | `+0.74%` | `neutral` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed` | `266157.192 ops/s` | `266275.783 ops/s` | `+0.04%` | `neutral` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:getWorkload` | `264706.839 ops/s` | `264778.926 ops/s` | `+0.03%` | `neutral` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:putWorkload` | `1450.353 ops/s` | `1496.857 ops/s` | `+3.21%` | `better` |
| `segment-index-persisted-mutation:deleteAsyncJoin` | `1965.911 ops/s` | `2522.792 ops/s` | `+28.33%` | `better` |
| `segment-index-persisted-mutation:deleteSync` | `2126.259 ops/s` | `2752.565 ops/s` | `+29.46%` | `better` |
| `segment-index-persisted-mutation:putAsyncJoin` | `1923.707 ops/s` | `2472.437 ops/s` | `+28.52%` | `better` |
| `segment-index-persisted-mutation:putSync` | `2115.026 ops/s` | `2724.260 ops/s` | `+28.81%` | `better` |
| `single-chunk-entry-write-compact:writeEntrySteadyState` | `8379548.778 ops/s` | `8313735.079 ops/s` | `-0.79%` | `neutral` |
| `single-chunk-entry-write-large:writeEntrySteadyState` | `7940359.689 ops/s` | `7840551.997 ops/s` | `-1.26%` | `neutral` |
| `sorted-data-diff-key-read-compact:readNextKey` | `9479790.837 ops/s` | `8269018.385 ops/s` | `-12.77%` | `worse` |
| `sorted-data-diff-key-read-large:readNextKey` | `7496036.998 ops/s` | `6771093.069 ops/s` | `-9.67%` | `worse` |
