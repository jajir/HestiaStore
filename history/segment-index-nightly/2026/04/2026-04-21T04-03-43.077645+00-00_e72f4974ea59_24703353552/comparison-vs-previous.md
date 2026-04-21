# Benchmark Comparison

- Candidate labels rerun 3x total and median-merged because the initial regression exceeded threshold with high relative score error: `segment-index-get-multisegment-cold,segment-index-mixed-split-heavy`


- Profile: `segment-index-nightly`
- Baseline SHA: `e72f4974ea597aa48d2725b131d13e39bcfa6872`
- Candidate SHA: `e72f4974ea597aa48d2725b131d13e39bcfa6872`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `segment-index-get-multisegment-cold:getHitAsyncJoin` | `44.520 ops/s` | `39.688 ops/s` | `-10.85%` | `worse` |
| `segment-index-get-multisegment-cold:getHitSync` | `51.691 ops/s` | `41.492 ops/s` | `-19.73%` | `worse` |
| `segment-index-get-multisegment-cold:getMissAsyncJoin` | `185808.037 ops/s` | `172280.988 ops/s` | `-7.28%` | `worse` |
| `segment-index-get-multisegment-cold:getMissSync` | `3529470.440 ops/s` | `3788045.074 ops/s` | `+7.33%` | `better` |
| `segment-index-get-multisegment-hot:getHitAsyncJoin` | `86.702 ops/s` | `96.060 ops/s` | `+10.79%` | `better` |
| `segment-index-get-multisegment-hot:getHitSync` | `93.352 ops/s` | `89.034 ops/s` | `-4.63%` | `warning` |
| `segment-index-get-multisegment-hot:getMissAsyncJoin` | `186478.063 ops/s` | `174035.108 ops/s` | `-6.67%` | `warning` |
| `segment-index-get-multisegment-hot:getMissSync` | `3652847.115 ops/s` | `4110898.319 ops/s` | `+12.54%` | `better` |
| `segment-index-get-overlay:getHitAsyncJoin` | `174669.780 ops/s` | `156786.029 ops/s` | `-10.24%` | `worse` |
| `segment-index-get-overlay:getHitSync` | `4042992.764 ops/s` | `4168576.415 ops/s` | `+3.11%` | `better` |
| `segment-index-get-overlay:getMissAsyncJoin` | `176435.049 ops/s` | `156286.985 ops/s` | `-11.42%` | `worse` |
| `segment-index-get-overlay:getMissSync` | `3905101.799 ops/s` | `3789194.220 ops/s` | `-2.97%` | `neutral` |
| `segment-index-get-persisted:getHitAsyncJoin` | `62740.491 ops/s` | `62056.903 ops/s` | `-1.09%` | `neutral` |
| `segment-index-get-persisted:getHitSync` | `114747.877 ops/s` | `115044.253 ops/s` | `+0.26%` | `neutral` |
| `segment-index-get-persisted:getMissAsyncJoin` | `176527.339 ops/s` | `164924.963 ops/s` | `-6.57%` | `warning` |
| `segment-index-get-persisted:getMissSync` | `3612638.802 ops/s` | `3875849.368 ops/s` | `+7.29%` | `better` |
| `segment-index-hot-partition-put:putHotPartition` | `2757806.926 ops/s` | `2815830.466 ops/s` | `+2.10%` | `neutral` |
| `segment-index-hot-partition-put:putThenGetHotPartition` | `1708021.486 ops/s` | `1669428.286 ops/s` | `-2.26%` | `neutral` |
| `segment-index-lifecycle:openAndCheckAndRepairConsistency` | `242.483 ms/op` | `243.557 ms/op` | `+0.44%` | `neutral` |
| `segment-index-lifecycle:openAndCompact` | `267.121 ms/op` | `264.710 ms/op` | `-0.90%` | `neutral` |
| `segment-index-lifecycle:openExisting` | `240.076 ms/op` | `241.316 ms/op` | `+0.52%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed` | `521119.046 ops/s` | `493513.460 ops/s` | `-5.30%` | `warning` |
| `segment-index-mixed-drain:partitionedIngestMixed:getWorkload` | `515778.022 ops/s` | `488145.421 ops/s` | `-5.36%` | `warning` |
| `segment-index-mixed-drain:partitionedIngestMixed:putWorkload` | `5341.024 ops/s` | `5368.039 ops/s` | `+0.51%` | `neutral` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed` | `321731.570 ops/s` | `263364.183 ops/s` | `-18.14%` | `worse` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:getWorkload` | `261212.805 ops/s` | `261873.515 ops/s` | `+0.25%` | `neutral` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:putWorkload` | `60518.765 ops/s` | `1490.668 ops/s` | `-97.54%` | `worse` |
| `segment-index-persisted-mutation:deleteAsyncJoin` | `1575.234 ops/s` | `1990.078 ops/s` | `+26.34%` | `better` |
| `segment-index-persisted-mutation:deleteSync` | `1744.843 ops/s` | `2146.076 ops/s` | `+23.00%` | `better` |
| `segment-index-persisted-mutation:putAsyncJoin` | `1632.265 ops/s` | `1913.809 ops/s` | `+17.25%` | `better` |
| `segment-index-persisted-mutation:putSync` | `1719.753 ops/s` | `2136.706 ops/s` | `+24.24%` | `better` |
| `single-chunk-entry-write-compact:writeEntrySteadyState` | `8468925.093 ops/s` | `8513860.599 ops/s` | `+0.53%` | `neutral` |
| `single-chunk-entry-write-large:writeEntrySteadyState` | `7954837.641 ops/s` | `7954295.350 ops/s` | `-0.01%` | `neutral` |
| `sorted-data-diff-key-read-compact:readNextKey` | `9141912.247 ops/s` | `9435940.265 ops/s` | `+3.22%` | `better` |
| `sorted-data-diff-key-read-large:readNextKey` | `7484988.885 ops/s` | `7482597.459 ops/s` | `-0.03%` | `neutral` |
