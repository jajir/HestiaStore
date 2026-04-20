# Benchmark Comparison

- Candidate labels rerun 3x total and median-merged because the initial regression exceeded threshold with high relative score error: `segment-index-get-multisegment-hot,segment-index-persisted-mutation`


- Profile: `segment-index-nightly`
- Baseline SHA: `e72f4974ea597aa48d2725b131d13e39bcfa6872`
- Candidate SHA: `e72f4974ea597aa48d2725b131d13e39bcfa6872`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `segment-index-get-multisegment-cold:getHitAsyncJoin` | `41.686 ops/s` | `44.520 ops/s` | `+6.80%` | `better` |
| `segment-index-get-multisegment-cold:getHitSync` | `43.094 ops/s` | `51.691 ops/s` | `+19.95%` | `better` |
| `segment-index-get-multisegment-cold:getMissAsyncJoin` | `169996.138 ops/s` | `185808.037 ops/s` | `+9.30%` | `better` |
| `segment-index-get-multisegment-cold:getMissSync` | `3590383.682 ops/s` | `3529470.440 ops/s` | `-1.70%` | `neutral` |
| `segment-index-get-multisegment-hot:getHitAsyncJoin` | `95.297 ops/s` | `86.702 ops/s` | `-9.02%` | `worse` |
| `segment-index-get-multisegment-hot:getHitSync` | `101.286 ops/s` | `93.352 ops/s` | `-7.83%` | `worse` |
| `segment-index-get-multisegment-hot:getMissAsyncJoin` | `170040.772 ops/s` | `186478.063 ops/s` | `+9.67%` | `better` |
| `segment-index-get-multisegment-hot:getMissSync` | `3661219.320 ops/s` | `3652847.115 ops/s` | `-0.23%` | `neutral` |
| `segment-index-get-overlay:getHitAsyncJoin` | `162741.357 ops/s` | `174669.780 ops/s` | `+7.33%` | `better` |
| `segment-index-get-overlay:getHitSync` | `3860435.698 ops/s` | `4042992.764 ops/s` | `+4.73%` | `better` |
| `segment-index-get-overlay:getMissAsyncJoin` | `163296.596 ops/s` | `176435.049 ops/s` | `+8.05%` | `better` |
| `segment-index-get-overlay:getMissSync` | `3533129.210 ops/s` | `3905101.799 ops/s` | `+10.53%` | `better` |
| `segment-index-get-persisted:getHitAsyncJoin` | `63300.796 ops/s` | `62740.491 ops/s` | `-0.89%` | `neutral` |
| `segment-index-get-persisted:getHitSync` | `118039.182 ops/s` | `114747.877 ops/s` | `-2.79%` | `neutral` |
| `segment-index-get-persisted:getMissAsyncJoin` | `164478.983 ops/s` | `176527.339 ops/s` | `+7.33%` | `better` |
| `segment-index-get-persisted:getMissSync` | `3737327.566 ops/s` | `3612638.802 ops/s` | `-3.34%` | `warning` |
| `segment-index-hot-partition-put:putHotPartition` | `3021395.060 ops/s` | `2757806.926 ops/s` | `-8.72%` | `worse` |
| `segment-index-hot-partition-put:putThenGetHotPartition` | `1655506.970 ops/s` | `1708021.486 ops/s` | `+3.17%` | `better` |
| `segment-index-lifecycle:openAndCheckAndRepairConsistency` | `247.968 ms/op` | `242.483 ms/op` | `-2.21%` | `neutral` |
| `segment-index-lifecycle:openAndCompact` | `268.891 ms/op` | `267.121 ms/op` | `-0.66%` | `neutral` |
| `segment-index-lifecycle:openExisting` | `246.062 ms/op` | `240.076 ms/op` | `-2.43%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed` | `507625.255 ops/s` | `521119.046 ops/s` | `+2.66%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed:getWorkload` | `502299.814 ops/s` | `515778.022 ops/s` | `+2.68%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed:putWorkload` | `5325.442 ops/s` | `5341.024 ops/s` | `+0.29%` | `neutral` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed` | `254412.825 ops/s` | `321731.570 ops/s` | `+26.46%` | `better` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:getWorkload` | `252950.967 ops/s` | `261212.805 ops/s` | `+3.27%` | `better` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:putWorkload` | `1461.858 ops/s` | `60518.765 ops/s` | `+4039.85%` | `better` |
| `segment-index-persisted-mutation:deleteAsyncJoin` | `2024.829 ops/s` | `1575.234 ops/s` | `-22.20%` | `worse` |
| `segment-index-persisted-mutation:deleteSync` | `2267.660 ops/s` | `1744.843 ops/s` | `-23.06%` | `worse` |
| `segment-index-persisted-mutation:putAsyncJoin` | `2012.892 ops/s` | `1632.265 ops/s` | `-18.91%` | `worse` |
| `segment-index-persisted-mutation:putSync` | `2176.649 ops/s` | `1719.753 ops/s` | `-20.99%` | `worse` |
| `single-chunk-entry-write-compact:writeEntrySteadyState` | `8549613.464 ops/s` | `8468925.093 ops/s` | `-0.94%` | `neutral` |
| `single-chunk-entry-write-large:writeEntrySteadyState` | `7855781.820 ops/s` | `7954837.641 ops/s` | `+1.26%` | `neutral` |
| `sorted-data-diff-key-read-compact:readNextKey` | `8689181.654 ops/s` | `9141912.247 ops/s` | `+5.21%` | `better` |
| `sorted-data-diff-key-read-large:readNextKey` | `7373826.343 ops/s` | `7484988.885 ops/s` | `+1.51%` | `neutral` |
