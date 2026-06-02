# Benchmark Comparison

- Candidate labels rerun 3x total and median-merged because the initial regression exceeded threshold with high relative score error: `segment-index-get-multisegment-cold`


- Profile: `segment-index-nightly`
- Baseline SHA: `e72f4974ea597aa48d2725b131d13e39bcfa6872`
- Candidate SHA: `e72f4974ea597aa48d2725b131d13e39bcfa6872`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `segment-index-get-multisegment-cold:getHitAsyncJoin` | `50.477 ops/s` | `46.254 ops/s` | `-8.37%` | `worse` |
| `segment-index-get-multisegment-cold:getHitSync` | `48.795 ops/s` | `38.224 ops/s` | `-21.67%` | `worse` |
| `segment-index-get-multisegment-cold:getMissAsyncJoin` | `171546.333 ops/s` | `189001.003 ops/s` | `+10.17%` | `better` |
| `segment-index-get-multisegment-cold:getMissSync` | `3504023.683 ops/s` | `3807131.025 ops/s` | `+8.65%` | `better` |
| `segment-index-get-multisegment-hot:getHitAsyncJoin` | `86.842 ops/s` | `83.553 ops/s` | `-3.79%` | `warning` |
| `segment-index-get-multisegment-hot:getHitSync` | `78.808 ops/s` | `98.622 ops/s` | `+25.14%` | `better` |
| `segment-index-get-multisegment-hot:getMissAsyncJoin` | `171997.189 ops/s` | `186986.895 ops/s` | `+8.72%` | `better` |
| `segment-index-get-multisegment-hot:getMissSync` | `3634622.857 ops/s` | `3612158.723 ops/s` | `-0.62%` | `neutral` |
| `segment-index-get-overlay:getHitAsyncJoin` | `163750.336 ops/s` | `178216.189 ops/s` | `+8.83%` | `better` |
| `segment-index-get-overlay:getHitSync` | `3943900.051 ops/s` | `4151553.481 ops/s` | `+5.27%` | `better` |
| `segment-index-get-overlay:getMissAsyncJoin` | `163157.590 ops/s` | `180209.289 ops/s` | `+10.45%` | `better` |
| `segment-index-get-overlay:getMissSync` | `3596652.290 ops/s` | `3940872.378 ops/s` | `+9.57%` | `better` |
| `segment-index-get-persisted:getHitAsyncJoin` | `59167.213 ops/s` | `60468.109 ops/s` | `+2.20%` | `neutral` |
| `segment-index-get-persisted:getHitSync` | `116949.483 ops/s` | `116973.322 ops/s` | `+0.02%` | `neutral` |
| `segment-index-get-persisted:getMissAsyncJoin` | `164942.156 ops/s` | `178833.170 ops/s` | `+8.42%` | `better` |
| `segment-index-get-persisted:getMissSync` | `3914720.510 ops/s` | `3663595.402 ops/s` | `-6.41%` | `warning` |
| `segment-index-hot-partition-put:putHotPartition` | `2871119.161 ops/s` | `2824145.022 ops/s` | `-1.64%` | `neutral` |
| `segment-index-hot-partition-put:putThenGetHotPartition` | `1558568.841 ops/s` | `1562762.820 ops/s` | `+0.27%` | `neutral` |
| `segment-index-lifecycle:openAndCheckAndRepairConsistency` | `245.828 ms/op` | `243.291 ms/op` | `-1.03%` | `neutral` |
| `segment-index-lifecycle:openAndCompact` | `268.390 ms/op` | `265.758 ms/op` | `-0.98%` | `neutral` |
| `segment-index-lifecycle:openExisting` | `244.120 ms/op` | `238.551 ms/op` | `-2.28%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed` | `503510.722 ops/s` | `543316.719 ops/s` | `+7.91%` | `better` |
| `segment-index-mixed-drain:partitionedIngestMixed:getWorkload` | `498190.291 ops/s` | `537961.494 ops/s` | `+7.98%` | `better` |
| `segment-index-mixed-drain:partitionedIngestMixed:putWorkload` | `5320.431 ops/s` | `5355.225 ops/s` | `+0.65%` | `neutral` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed` | `265329.273 ops/s` | `262034.547 ops/s` | `-1.24%` | `neutral` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:getWorkload` | `263798.759 ops/s` | `260531.462 ops/s` | `-1.24%` | `neutral` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:putWorkload` | `1530.514 ops/s` | `1503.085 ops/s` | `-1.79%` | `neutral` |
| `segment-index-persisted-mutation:deleteAsyncJoin` | `1996.147 ops/s` | `2169.417 ops/s` | `+8.68%` | `better` |
| `segment-index-persisted-mutation:deleteSync` | `2219.076 ops/s` | `2124.968 ops/s` | `-4.24%` | `warning` |
| `segment-index-persisted-mutation:putAsyncJoin` | `1969.064 ops/s` | `2018.631 ops/s` | `+2.52%` | `neutral` |
| `segment-index-persisted-mutation:putSync` | `2178.491 ops/s` | `2174.391 ops/s` | `-0.19%` | `neutral` |
| `single-chunk-entry-write-compact:writeEntrySteadyState` | `8480858.639 ops/s` | `8405295.659 ops/s` | `-0.89%` | `neutral` |
| `single-chunk-entry-write-large:writeEntrySteadyState` | `7945338.216 ops/s` | `7934656.733 ops/s` | `-0.13%` | `neutral` |
| `sorted-data-diff-key-read-compact:readNextKey` | `9047526.119 ops/s` | `9098212.561 ops/s` | `+0.56%` | `neutral` |
| `sorted-data-diff-key-read-large:readNextKey` | `7445410.546 ops/s` | `7201907.041 ops/s` | `-3.27%` | `warning` |
