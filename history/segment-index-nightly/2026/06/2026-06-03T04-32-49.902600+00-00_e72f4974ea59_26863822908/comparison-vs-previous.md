# Benchmark Comparison

- Candidate labels rerun 3x total and median-merged because the initial regression exceeded threshold with high relative score error: `segment-index-get-multisegment-hot,segment-index-persisted-mutation`


- Profile: `segment-index-nightly`
- Baseline SHA: `e72f4974ea597aa48d2725b131d13e39bcfa6872`
- Candidate SHA: `e72f4974ea597aa48d2725b131d13e39bcfa6872`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `segment-index-get-multisegment-cold:getHitAsyncJoin` | `46.254 ops/s` | `54.032 ops/s` | `+16.82%` | `better` |
| `segment-index-get-multisegment-cold:getHitSync` | `38.224 ops/s` | `38.605 ops/s` | `+1.00%` | `neutral` |
| `segment-index-get-multisegment-cold:getMissAsyncJoin` | `189001.003 ops/s` | `167641.298 ops/s` | `-11.30%` | `worse` |
| `segment-index-get-multisegment-cold:getMissSync` | `3807131.025 ops/s` | `3597563.854 ops/s` | `-5.50%` | `warning` |
| `segment-index-get-multisegment-hot:getHitAsyncJoin` | `83.553 ops/s` | `95.429 ops/s` | `+14.21%` | `better` |
| `segment-index-get-multisegment-hot:getHitSync` | `98.622 ops/s` | `89.916 ops/s` | `-8.83%` | `worse` |
| `segment-index-get-multisegment-hot:getMissAsyncJoin` | `186986.895 ops/s` | `168028.701 ops/s` | `-10.14%` | `worse` |
| `segment-index-get-multisegment-hot:getMissSync` | `3612158.723 ops/s` | `3587021.755 ops/s` | `-0.70%` | `neutral` |
| `segment-index-get-overlay:getHitAsyncJoin` | `178216.189 ops/s` | `159643.937 ops/s` | `-10.42%` | `worse` |
| `segment-index-get-overlay:getHitSync` | `4151553.481 ops/s` | `3985592.079 ops/s` | `-4.00%` | `warning` |
| `segment-index-get-overlay:getMissAsyncJoin` | `180209.289 ops/s` | `161112.748 ops/s` | `-10.60%` | `worse` |
| `segment-index-get-overlay:getMissSync` | `3940872.378 ops/s` | `3683736.361 ops/s` | `-6.52%` | `warning` |
| `segment-index-get-persisted:getHitAsyncJoin` | `60468.109 ops/s` | `57593.471 ops/s` | `-4.75%` | `warning` |
| `segment-index-get-persisted:getHitSync` | `116973.322 ops/s` | `115434.722 ops/s` | `-1.32%` | `neutral` |
| `segment-index-get-persisted:getMissAsyncJoin` | `178833.170 ops/s` | `163553.947 ops/s` | `-8.54%` | `worse` |
| `segment-index-get-persisted:getMissSync` | `3663595.402 ops/s` | `3704959.205 ops/s` | `+1.13%` | `neutral` |
| `segment-index-hot-partition-put:putHotPartition` | `2824145.022 ops/s` | `2734767.609 ops/s` | `-3.16%` | `warning` |
| `segment-index-hot-partition-put:putThenGetHotPartition` | `1562762.820 ops/s` | `1657761.100 ops/s` | `+6.08%` | `better` |
| `segment-index-lifecycle:openAndCheckAndRepairConsistency` | `243.291 ms/op` | `249.049 ms/op` | `+2.37%` | `neutral` |
| `segment-index-lifecycle:openAndCompact` | `265.758 ms/op` | `271.475 ms/op` | `+2.15%` | `neutral` |
| `segment-index-lifecycle:openExisting` | `238.551 ms/op` | `246.168 ms/op` | `+3.19%` | `better` |
| `segment-index-mixed-drain:partitionedIngestMixed` | `543316.719 ops/s` | `490104.772 ops/s` | `-9.79%` | `worse` |
| `segment-index-mixed-drain:partitionedIngestMixed:getWorkload` | `537961.494 ops/s` | `484775.813 ops/s` | `-9.89%` | `worse` |
| `segment-index-mixed-drain:partitionedIngestMixed:putWorkload` | `5355.225 ops/s` | `5328.959 ops/s` | `-0.49%` | `neutral` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed` | `262034.547 ops/s` | `316711.176 ops/s` | `+20.87%` | `better` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:getWorkload` | `260531.462 ops/s` | `263547.305 ops/s` | `+1.16%` | `neutral` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:putWorkload` | `1503.085 ops/s` | `53163.871 ops/s` | `+3436.98%` | `better` |
| `segment-index-persisted-mutation:deleteAsyncJoin` | `2169.417 ops/s` | `1970.968 ops/s` | `-9.15%` | `worse` |
| `segment-index-persisted-mutation:deleteSync` | `2124.968 ops/s` | `2124.232 ops/s` | `-0.03%` | `neutral` |
| `segment-index-persisted-mutation:putAsyncJoin` | `2018.631 ops/s` | `1899.660 ops/s` | `-5.89%` | `warning` |
| `segment-index-persisted-mutation:putSync` | `2174.391 ops/s` | `2059.382 ops/s` | `-5.29%` | `warning` |
| `single-chunk-entry-write-compact:writeEntrySteadyState` | `8405295.659 ops/s` | `8434797.215 ops/s` | `+0.35%` | `neutral` |
| `single-chunk-entry-write-large:writeEntrySteadyState` | `7934656.733 ops/s` | `7820212.013 ops/s` | `-1.44%` | `neutral` |
| `sorted-data-diff-key-read-compact:readNextKey` | `9098212.561 ops/s` | `8567140.421 ops/s` | `-5.84%` | `warning` |
| `sorted-data-diff-key-read-large:readNextKey` | `7201907.041 ops/s` | `6989408.013 ops/s` | `-2.95%` | `neutral` |
