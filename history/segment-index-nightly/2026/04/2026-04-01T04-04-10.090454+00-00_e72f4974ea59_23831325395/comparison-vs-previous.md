# Benchmark Comparison

- Candidate labels rerun 3x total and median-merged because the initial regression exceeded threshold with high relative score error: `segment-index-get-multisegment-cold,segment-index-get-multisegment-hot`


- Profile: `segment-index-nightly`
- Baseline SHA: `e72f4974ea597aa48d2725b131d13e39bcfa6872`
- Candidate SHA: `e72f4974ea597aa48d2725b131d13e39bcfa6872`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `segment-index-get-multisegment-cold:getHitAsyncJoin` | `42.426 ops/s` | `39.812 ops/s` | `-6.16%` | `warning` |
| `segment-index-get-multisegment-cold:getHitSync` | `48.495 ops/s` | `40.164 ops/s` | `-17.18%` | `worse` |
| `segment-index-get-multisegment-cold:getMissAsyncJoin` | `180390.293 ops/s` | `188779.260 ops/s` | `+4.65%` | `better` |
| `segment-index-get-multisegment-cold:getMissSync` | `3662235.407 ops/s` | `3540146.339 ops/s` | `-3.33%` | `warning` |
| `segment-index-get-multisegment-hot:getHitAsyncJoin` | `102.066 ops/s` | `83.182 ops/s` | `-18.50%` | `worse` |
| `segment-index-get-multisegment-hot:getHitSync` | `98.613 ops/s` | `91.824 ops/s` | `-6.88%` | `warning` |
| `segment-index-get-multisegment-hot:getMissAsyncJoin` | `180900.758 ops/s` | `189088.980 ops/s` | `+4.53%` | `better` |
| `segment-index-get-multisegment-hot:getMissSync` | `3972218.136 ops/s` | `3835410.012 ops/s` | `-3.44%` | `warning` |
| `segment-index-get-overlay:getHitAsyncJoin` | `168911.345 ops/s` | `171011.092 ops/s` | `+1.24%` | `neutral` |
| `segment-index-get-overlay:getHitSync` | `3879716.137 ops/s` | `3969786.693 ops/s` | `+2.32%` | `neutral` |
| `segment-index-get-overlay:getMissAsyncJoin` | `167957.216 ops/s` | `177136.476 ops/s` | `+5.47%` | `better` |
| `segment-index-get-overlay:getMissSync` | `3678663.268 ops/s` | `3865928.060 ops/s` | `+5.09%` | `better` |
| `segment-index-get-persisted:getHitAsyncJoin` | `65153.269 ops/s` | `64280.155 ops/s` | `-1.34%` | `neutral` |
| `segment-index-get-persisted:getHitSync` | `118082.223 ops/s` | `112586.471 ops/s` | `-4.65%` | `warning` |
| `segment-index-get-persisted:getMissAsyncJoin` | `164585.026 ops/s` | `178174.940 ops/s` | `+8.26%` | `better` |
| `segment-index-get-persisted:getMissSync` | `3655872.252 ops/s` | `3979264.559 ops/s` | `+8.85%` | `better` |
| `segment-index-hot-partition-put:putHotPartition` | `2875224.845 ops/s` | `2875413.577 ops/s` | `+0.01%` | `neutral` |
| `segment-index-hot-partition-put:putThenGetHotPartition` | `1652642.212 ops/s` | `1746949.733 ops/s` | `+5.71%` | `better` |
| `segment-index-lifecycle:openAndCheckAndRepairConsistency` | `244.789 ms/op` | `245.650 ms/op` | `+0.35%` | `neutral` |
| `segment-index-lifecycle:openAndCompact` | `268.778 ms/op` | `265.398 ms/op` | `-1.26%` | `neutral` |
| `segment-index-lifecycle:openExisting` | `241.139 ms/op` | `240.983 ms/op` | `-0.06%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed` | `504631.958 ops/s` | `518482.343 ops/s` | `+2.74%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed:getWorkload` | `499348.273 ops/s` | `513172.084 ops/s` | `+2.77%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed:putWorkload` | `5283.685 ops/s` | `5310.260 ops/s` | `+0.50%` | `neutral` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed` | `269459.359 ops/s` | `264914.546 ops/s` | `-1.69%` | `neutral` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:getWorkload` | `268022.847 ops/s` | `263514.215 ops/s` | `-1.68%` | `neutral` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:putWorkload` | `1436.512 ops/s` | `1400.331 ops/s` | `-2.52%` | `neutral` |
| `segment-index-persisted-mutation:deleteAsyncJoin` | `2069.090 ops/s` | `1685.540 ops/s` | `-18.54%` | `worse` |
| `segment-index-persisted-mutation:deleteSync` | `2297.408 ops/s` | `1768.269 ops/s` | `-23.03%` | `worse` |
| `segment-index-persisted-mutation:putAsyncJoin` | `2029.158 ops/s` | `1663.651 ops/s` | `-18.01%` | `worse` |
| `segment-index-persisted-mutation:putSync` | `2250.612 ops/s` | `1833.511 ops/s` | `-18.53%` | `worse` |
| `single-chunk-entry-write-compact:writeEntrySteadyState` | `8517563.106 ops/s` | `8592372.744 ops/s` | `+0.88%` | `neutral` |
| `single-chunk-entry-write-large:writeEntrySteadyState` | `7933211.638 ops/s` | `7970378.242 ops/s` | `+0.47%` | `neutral` |
| `sorted-data-diff-key-read-compact:readNextKey` | `9400514.844 ops/s` | `9396918.289 ops/s` | `-0.04%` | `neutral` |
| `sorted-data-diff-key-read-large:readNextKey` | `7588164.758 ops/s` | `7151502.761 ops/s` | `-5.75%` | `warning` |
