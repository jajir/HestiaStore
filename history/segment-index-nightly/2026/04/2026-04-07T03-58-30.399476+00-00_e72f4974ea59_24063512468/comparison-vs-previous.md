# Benchmark Comparison

- Candidate labels rerun 3x total and median-merged because the initial regression exceeded threshold with high relative score error: `segment-index-get-multisegment-cold,segment-index-get-multisegment-hot`


- Profile: `segment-index-nightly`
- Baseline SHA: `e72f4974ea597aa48d2725b131d13e39bcfa6872`
- Candidate SHA: `e72f4974ea597aa48d2725b131d13e39bcfa6872`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `segment-index-get-multisegment-cold:getHitAsyncJoin` | `48.180 ops/s` | `41.227 ops/s` | `-14.43%` | `worse` |
| `segment-index-get-multisegment-cold:getHitSync` | `52.127 ops/s` | `48.833 ops/s` | `-6.32%` | `warning` |
| `segment-index-get-multisegment-cold:getMissAsyncJoin` | `187335.136 ops/s` | `168898.081 ops/s` | `-9.84%` | `worse` |
| `segment-index-get-multisegment-cold:getMissSync` | `4000298.175 ops/s` | `3732416.378 ops/s` | `-6.70%` | `warning` |
| `segment-index-get-multisegment-hot:getHitAsyncJoin` | `102.181 ops/s` | `110.741 ops/s` | `+8.38%` | `better` |
| `segment-index-get-multisegment-hot:getHitSync` | `98.103 ops/s` | `89.899 ops/s` | `-8.36%` | `worse` |
| `segment-index-get-multisegment-hot:getMissAsyncJoin` | `186610.608 ops/s` | `168896.909 ops/s` | `-9.49%` | `worse` |
| `segment-index-get-multisegment-hot:getMissSync` | `3397711.316 ops/s` | `3737556.372 ops/s` | `+10.00%` | `better` |
| `segment-index-get-overlay:getHitAsyncJoin` | `177603.136 ops/s` | `159576.937 ops/s` | `-10.15%` | `worse` |
| `segment-index-get-overlay:getHitSync` | `4088941.920 ops/s` | `4008933.426 ops/s` | `-1.96%` | `neutral` |
| `segment-index-get-overlay:getMissAsyncJoin` | `170722.316 ops/s` | `159579.492 ops/s` | `-6.53%` | `warning` |
| `segment-index-get-overlay:getMissSync` | `3988139.965 ops/s` | `3931700.745 ops/s` | `-1.42%` | `neutral` |
| `segment-index-get-persisted:getHitAsyncJoin` | `64450.912 ops/s` | `63605.561 ops/s` | `-1.31%` | `neutral` |
| `segment-index-get-persisted:getHitSync` | `115735.090 ops/s` | `117233.946 ops/s` | `+1.30%` | `neutral` |
| `segment-index-get-persisted:getMissAsyncJoin` | `168061.301 ops/s` | `162149.450 ops/s` | `-3.52%` | `warning` |
| `segment-index-get-persisted:getMissSync` | `3878344.569 ops/s` | `3405313.619 ops/s` | `-12.20%` | `worse` |
| `segment-index-hot-partition-put:putHotPartition` | `2802978.340 ops/s` | `3086276.349 ops/s` | `+10.11%` | `better` |
| `segment-index-hot-partition-put:putThenGetHotPartition` | `1515864.843 ops/s` | `1474284.505 ops/s` | `-2.74%` | `neutral` |
| `segment-index-lifecycle:openAndCheckAndRepairConsistency` | `246.643 ms/op` | `249.489 ms/op` | `+1.15%` | `neutral` |
| `segment-index-lifecycle:openAndCompact` | `267.744 ms/op` | `269.947 ms/op` | `+0.82%` | `neutral` |
| `segment-index-lifecycle:openExisting` | `242.596 ms/op` | `245.312 ms/op` | `+1.12%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed` | `517569.002 ops/s` | `506613.243 ops/s` | `-2.12%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed:getWorkload` | `512269.454 ops/s` | `501328.529 ops/s` | `-2.14%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed:putWorkload` | `5299.547 ops/s` | `5284.714 ops/s` | `-0.28%` | `neutral` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed` | `268061.761 ops/s` | `260572.296 ops/s` | `-2.79%` | `neutral` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:getWorkload` | `266479.119 ops/s` | `259146.775 ops/s` | `-2.75%` | `neutral` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:putWorkload` | `1582.641 ops/s` | `1425.521 ops/s` | `-9.93%` | `worse` |
| `segment-index-persisted-mutation:deleteAsyncJoin` | `2064.825 ops/s` | `1989.316 ops/s` | `-3.66%` | `warning` |
| `segment-index-persisted-mutation:deleteSync` | `2269.412 ops/s` | `2204.342 ops/s` | `-2.87%` | `neutral` |
| `segment-index-persisted-mutation:putAsyncJoin` | `2022.553 ops/s` | `1959.368 ops/s` | `-3.12%` | `warning` |
| `segment-index-persisted-mutation:putSync` | `2163.975 ops/s` | `2180.103 ops/s` | `+0.75%` | `neutral` |
| `single-chunk-entry-write-compact:writeEntrySteadyState` | `8564881.075 ops/s` | `8519375.377 ops/s` | `-0.53%` | `neutral` |
| `single-chunk-entry-write-large:writeEntrySteadyState` | `7933883.434 ops/s` | `7814387.152 ops/s` | `-1.51%` | `neutral` |
| `sorted-data-diff-key-read-compact:readNextKey` | `9165662.296 ops/s` | `8946622.244 ops/s` | `-2.39%` | `neutral` |
| `sorted-data-diff-key-read-large:readNextKey` | `7414532.052 ops/s` | `7400117.839 ops/s` | `-0.19%` | `neutral` |
