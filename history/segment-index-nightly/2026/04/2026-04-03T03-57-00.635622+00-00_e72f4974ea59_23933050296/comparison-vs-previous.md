# Benchmark Comparison

- Candidate labels rerun 3x total and median-merged because the initial regression exceeded threshold with high relative score error: `segment-index-get-multisegment-hot`


- Profile: `segment-index-nightly`
- Baseline SHA: `e72f4974ea597aa48d2725b131d13e39bcfa6872`
- Candidate SHA: `e72f4974ea597aa48d2725b131d13e39bcfa6872`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `segment-index-get-multisegment-cold:getHitAsyncJoin` | `48.319 ops/s` | `54.522 ops/s` | `+12.84%` | `better` |
| `segment-index-get-multisegment-cold:getHitSync` | `41.870 ops/s` | `56.570 ops/s` | `+35.11%` | `better` |
| `segment-index-get-multisegment-cold:getMissAsyncJoin` | `169925.318 ops/s` | `170547.616 ops/s` | `+0.37%` | `neutral` |
| `segment-index-get-multisegment-cold:getMissSync` | `3870537.068 ops/s` | `4028489.029 ops/s` | `+4.08%` | `better` |
| `segment-index-get-multisegment-hot:getHitAsyncJoin` | `94.577 ops/s` | `94.350 ops/s` | `-0.24%` | `neutral` |
| `segment-index-get-multisegment-hot:getHitSync` | `101.947 ops/s` | `88.830 ops/s` | `-12.87%` | `worse` |
| `segment-index-get-multisegment-hot:getMissAsyncJoin` | `173209.827 ops/s` | `170848.708 ops/s` | `-1.36%` | `neutral` |
| `segment-index-get-multisegment-hot:getMissSync` | `3970790.300 ops/s` | `3530093.457 ops/s` | `-11.10%` | `worse` |
| `segment-index-get-overlay:getHitAsyncJoin` | `164237.987 ops/s` | `166171.322 ops/s` | `+1.18%` | `neutral` |
| `segment-index-get-overlay:getHitSync` | `4038518.760 ops/s` | `4006278.896 ops/s` | `-0.80%` | `neutral` |
| `segment-index-get-overlay:getMissAsyncJoin` | `156911.269 ops/s` | `162818.997 ops/s` | `+3.77%` | `better` |
| `segment-index-get-overlay:getMissSync` | `3711945.957 ops/s` | `3613159.040 ops/s` | `-2.66%` | `neutral` |
| `segment-index-get-persisted:getHitAsyncJoin` | `64581.341 ops/s` | `65752.932 ops/s` | `+1.81%` | `neutral` |
| `segment-index-get-persisted:getHitSync` | `114565.826 ops/s` | `116849.407 ops/s` | `+1.99%` | `neutral` |
| `segment-index-get-persisted:getMissAsyncJoin` | `167055.516 ops/s` | `156722.374 ops/s` | `-6.19%` | `warning` |
| `segment-index-get-persisted:getMissSync` | `3776283.596 ops/s` | `4043149.258 ops/s` | `+7.07%` | `better` |
| `segment-index-hot-partition-put:putHotPartition` | `2669489.902 ops/s` | `3126026.231 ops/s` | `+17.10%` | `better` |
| `segment-index-hot-partition-put:putThenGetHotPartition` | `1621139.000 ops/s` | `1765939.775 ops/s` | `+8.93%` | `better` |
| `segment-index-lifecycle:openAndCheckAndRepairConsistency` | `243.955 ms/op` | `246.250 ms/op` | `+0.94%` | `neutral` |
| `segment-index-lifecycle:openAndCompact` | `266.020 ms/op` | `267.023 ms/op` | `+0.38%` | `neutral` |
| `segment-index-lifecycle:openExisting` | `242.815 ms/op` | `240.587 ms/op` | `-0.92%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed` | `508752.593 ops/s` | `508138.538 ops/s` | `-0.12%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed:getWorkload` | `503401.419 ops/s` | `502792.857 ops/s` | `-0.12%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed:putWorkload` | `5351.175 ops/s` | `5345.681 ops/s` | `-0.10%` | `neutral` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed` | `266778.540 ops/s` | `266948.957 ops/s` | `+0.06%` | `neutral` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:getWorkload` | `265295.273 ops/s` | `265553.481 ops/s` | `+0.10%` | `neutral` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:putWorkload` | `1483.268 ops/s` | `1395.476 ops/s` | `-5.92%` | `warning` |
| `segment-index-persisted-mutation:deleteAsyncJoin` | `2005.842 ops/s` | `2076.296 ops/s` | `+3.51%` | `better` |
| `segment-index-persisted-mutation:deleteSync` | `2189.098 ops/s` | `2261.685 ops/s` | `+3.32%` | `better` |
| `segment-index-persisted-mutation:putAsyncJoin` | `1991.159 ops/s` | `2005.416 ops/s` | `+0.72%` | `neutral` |
| `segment-index-persisted-mutation:putSync` | `2127.667 ops/s` | `2202.117 ops/s` | `+3.50%` | `better` |
| `single-chunk-entry-write-compact:writeEntrySteadyState` | `8497075.658 ops/s` | `8442677.732 ops/s` | `-0.64%` | `neutral` |
| `single-chunk-entry-write-large:writeEntrySteadyState` | `7960780.720 ops/s` | `7528829.686 ops/s` | `-5.43%` | `warning` |
| `sorted-data-diff-key-read-compact:readNextKey` | `9525690.306 ops/s` | `8572850.211 ops/s` | `-10.00%` | `worse` |
| `sorted-data-diff-key-read-large:readNextKey` | `7522974.772 ops/s` | `7523282.181 ops/s` | `+0.00%` | `neutral` |
