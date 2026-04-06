# Benchmark Comparison

- Candidate labels rerun 3x total and median-merged because the initial regression exceeded threshold with high relative score error: `segment-index-get-multisegment-cold,segment-index-get-persisted`


- Profile: `segment-index-nightly`
- Baseline SHA: `e72f4974ea597aa48d2725b131d13e39bcfa6872`
- Candidate SHA: `e72f4974ea597aa48d2725b131d13e39bcfa6872`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `segment-index-get-multisegment-cold:getHitAsyncJoin` | `46.999 ops/s` | `48.180 ops/s` | `+2.51%` | `neutral` |
| `segment-index-get-multisegment-cold:getHitSync` | `51.892 ops/s` | `52.127 ops/s` | `+0.45%` | `neutral` |
| `segment-index-get-multisegment-cold:getMissAsyncJoin` | `173625.546 ops/s` | `187335.136 ops/s` | `+7.90%` | `better` |
| `segment-index-get-multisegment-cold:getMissSync` | `3757845.033 ops/s` | `4000298.175 ops/s` | `+6.45%` | `better` |
| `segment-index-get-multisegment-hot:getHitAsyncJoin` | `88.137 ops/s` | `102.181 ops/s` | `+15.93%` | `better` |
| `segment-index-get-multisegment-hot:getHitSync` | `81.539 ops/s` | `98.103 ops/s` | `+20.31%` | `better` |
| `segment-index-get-multisegment-hot:getMissAsyncJoin` | `174090.688 ops/s` | `186610.608 ops/s` | `+7.19%` | `better` |
| `segment-index-get-multisegment-hot:getMissSync` | `3599783.794 ops/s` | `3397711.316 ops/s` | `-5.61%` | `warning` |
| `segment-index-get-overlay:getHitAsyncJoin` | `166343.451 ops/s` | `177603.136 ops/s` | `+6.77%` | `better` |
| `segment-index-get-overlay:getHitSync` | `3761976.887 ops/s` | `4088941.920 ops/s` | `+8.69%` | `better` |
| `segment-index-get-overlay:getMissAsyncJoin` | `159320.947 ops/s` | `170722.316 ops/s` | `+7.16%` | `better` |
| `segment-index-get-overlay:getMissSync` | `4064048.295 ops/s` | `3988139.965 ops/s` | `-1.87%` | `neutral` |
| `segment-index-get-persisted:getHitAsyncJoin` | `64785.488 ops/s` | `64450.912 ops/s` | `-0.52%` | `neutral` |
| `segment-index-get-persisted:getHitSync` | `111726.134 ops/s` | `115735.090 ops/s` | `+3.59%` | `better` |
| `segment-index-get-persisted:getMissAsyncJoin` | `166904.343 ops/s` | `168061.301 ops/s` | `+0.69%` | `neutral` |
| `segment-index-get-persisted:getMissSync` | `4364538.962 ops/s` | `3878344.569 ops/s` | `-11.14%` | `worse` |
| `segment-index-hot-partition-put:putHotPartition` | `3382076.732 ops/s` | `2802978.340 ops/s` | `-17.12%` | `worse` |
| `segment-index-hot-partition-put:putThenGetHotPartition` | `1669005.659 ops/s` | `1515864.843 ops/s` | `-9.18%` | `worse` |
| `segment-index-lifecycle:openAndCheckAndRepairConsistency` | `244.429 ms/op` | `246.643 ms/op` | `+0.91%` | `neutral` |
| `segment-index-lifecycle:openAndCompact` | `266.992 ms/op` | `267.744 ms/op` | `+0.28%` | `neutral` |
| `segment-index-lifecycle:openExisting` | `240.608 ms/op` | `242.596 ms/op` | `+0.83%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed` | `516290.416 ops/s` | `517569.002 ops/s` | `+0.25%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed:getWorkload` | `510993.170 ops/s` | `512269.454 ops/s` | `+0.25%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed:putWorkload` | `5297.246 ops/s` | `5299.547 ops/s` | `+0.04%` | `neutral` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed` | `264166.275 ops/s` | `268061.761 ops/s` | `+1.47%` | `neutral` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:getWorkload` | `262590.201 ops/s` | `266479.119 ops/s` | `+1.48%` | `neutral` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:putWorkload` | `1576.075 ops/s` | `1582.641 ops/s` | `+0.42%` | `neutral` |
| `segment-index-persisted-mutation:deleteAsyncJoin` | `1996.984 ops/s` | `2064.825 ops/s` | `+3.40%` | `better` |
| `segment-index-persisted-mutation:deleteSync` | `2153.552 ops/s` | `2269.412 ops/s` | `+5.38%` | `better` |
| `segment-index-persisted-mutation:putAsyncJoin` | `1928.439 ops/s` | `2022.553 ops/s` | `+4.88%` | `better` |
| `segment-index-persisted-mutation:putSync` | `2172.662 ops/s` | `2163.975 ops/s` | `-0.40%` | `neutral` |
| `single-chunk-entry-write-compact:writeEntrySteadyState` | `8511133.331 ops/s` | `8564881.075 ops/s` | `+0.63%` | `neutral` |
| `single-chunk-entry-write-large:writeEntrySteadyState` | `7950112.736 ops/s` | `7933883.434 ops/s` | `-0.20%` | `neutral` |
| `sorted-data-diff-key-read-compact:readNextKey` | `9056191.936 ops/s` | `9165662.296 ops/s` | `+1.21%` | `neutral` |
| `sorted-data-diff-key-read-large:readNextKey` | `7571941.212 ops/s` | `7414532.052 ops/s` | `-2.08%` | `neutral` |
