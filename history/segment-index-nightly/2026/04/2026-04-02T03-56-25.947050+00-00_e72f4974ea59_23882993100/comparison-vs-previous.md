# Benchmark Comparison

- Profile: `segment-index-nightly`
- Baseline SHA: `e72f4974ea597aa48d2725b131d13e39bcfa6872`
- Candidate SHA: `e72f4974ea597aa48d2725b131d13e39bcfa6872`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `segment-index-get-multisegment-cold:getHitAsyncJoin` | `39.812 ops/s` | `48.319 ops/s` | `+21.37%` | `better` |
| `segment-index-get-multisegment-cold:getHitSync` | `40.164 ops/s` | `41.870 ops/s` | `+4.25%` | `better` |
| `segment-index-get-multisegment-cold:getMissAsyncJoin` | `188779.260 ops/s` | `169925.318 ops/s` | `-9.99%` | `worse` |
| `segment-index-get-multisegment-cold:getMissSync` | `3540146.339 ops/s` | `3870537.068 ops/s` | `+9.33%` | `better` |
| `segment-index-get-multisegment-hot:getHitAsyncJoin` | `83.182 ops/s` | `94.577 ops/s` | `+13.70%` | `better` |
| `segment-index-get-multisegment-hot:getHitSync` | `91.824 ops/s` | `101.947 ops/s` | `+11.02%` | `better` |
| `segment-index-get-multisegment-hot:getMissAsyncJoin` | `189088.980 ops/s` | `173209.827 ops/s` | `-8.40%` | `worse` |
| `segment-index-get-multisegment-hot:getMissSync` | `3835410.012 ops/s` | `3970790.300 ops/s` | `+3.53%` | `better` |
| `segment-index-get-overlay:getHitAsyncJoin` | `171011.092 ops/s` | `164237.987 ops/s` | `-3.96%` | `warning` |
| `segment-index-get-overlay:getHitSync` | `3969786.693 ops/s` | `4038518.760 ops/s` | `+1.73%` | `neutral` |
| `segment-index-get-overlay:getMissAsyncJoin` | `177136.476 ops/s` | `156911.269 ops/s` | `-11.42%` | `worse` |
| `segment-index-get-overlay:getMissSync` | `3865928.060 ops/s` | `3711945.957 ops/s` | `-3.98%` | `warning` |
| `segment-index-get-persisted:getHitAsyncJoin` | `64280.155 ops/s` | `64581.341 ops/s` | `+0.47%` | `neutral` |
| `segment-index-get-persisted:getHitSync` | `112586.471 ops/s` | `114565.826 ops/s` | `+1.76%` | `neutral` |
| `segment-index-get-persisted:getMissAsyncJoin` | `178174.940 ops/s` | `167055.516 ops/s` | `-6.24%` | `warning` |
| `segment-index-get-persisted:getMissSync` | `3979264.559 ops/s` | `3776283.596 ops/s` | `-5.10%` | `warning` |
| `segment-index-hot-partition-put:putHotPartition` | `2875413.577 ops/s` | `2669489.902 ops/s` | `-7.16%` | `worse` |
| `segment-index-hot-partition-put:putThenGetHotPartition` | `1746949.733 ops/s` | `1621139.000 ops/s` | `-7.20%` | `worse` |
| `segment-index-lifecycle:openAndCheckAndRepairConsistency` | `245.650 ms/op` | `243.955 ms/op` | `-0.69%` | `neutral` |
| `segment-index-lifecycle:openAndCompact` | `265.398 ms/op` | `266.020 ms/op` | `+0.23%` | `neutral` |
| `segment-index-lifecycle:openExisting` | `240.983 ms/op` | `242.815 ms/op` | `+0.76%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed` | `518482.343 ops/s` | `508752.593 ops/s` | `-1.88%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed:getWorkload` | `513172.084 ops/s` | `503401.419 ops/s` | `-1.90%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed:putWorkload` | `5310.260 ops/s` | `5351.175 ops/s` | `+0.77%` | `neutral` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed` | `264914.546 ops/s` | `266778.540 ops/s` | `+0.70%` | `neutral` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:getWorkload` | `263514.215 ops/s` | `265295.273 ops/s` | `+0.68%` | `neutral` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:putWorkload` | `1400.331 ops/s` | `1483.268 ops/s` | `+5.92%` | `better` |
| `segment-index-persisted-mutation:deleteAsyncJoin` | `1685.540 ops/s` | `2005.842 ops/s` | `+19.00%` | `better` |
| `segment-index-persisted-mutation:deleteSync` | `1768.269 ops/s` | `2189.098 ops/s` | `+23.80%` | `better` |
| `segment-index-persisted-mutation:putAsyncJoin` | `1663.651 ops/s` | `1991.159 ops/s` | `+19.69%` | `better` |
| `segment-index-persisted-mutation:putSync` | `1833.511 ops/s` | `2127.667 ops/s` | `+16.04%` | `better` |
| `single-chunk-entry-write-compact:writeEntrySteadyState` | `8592372.744 ops/s` | `8497075.658 ops/s` | `-1.11%` | `neutral` |
| `single-chunk-entry-write-large:writeEntrySteadyState` | `7970378.242 ops/s` | `7960780.720 ops/s` | `-0.12%` | `neutral` |
| `sorted-data-diff-key-read-compact:readNextKey` | `9396918.289 ops/s` | `9525690.306 ops/s` | `+1.37%` | `neutral` |
| `sorted-data-diff-key-read-large:readNextKey` | `7151502.761 ops/s` | `7522974.772 ops/s` | `+5.19%` | `better` |
