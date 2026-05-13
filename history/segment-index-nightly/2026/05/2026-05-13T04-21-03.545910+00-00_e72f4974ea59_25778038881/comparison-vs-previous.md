# Benchmark Comparison

- Candidate labels rerun 3x total and median-merged because the initial regression exceeded threshold with high relative score error: `segment-index-get-multisegment-cold`


- Profile: `segment-index-nightly`
- Baseline SHA: `e72f4974ea597aa48d2725b131d13e39bcfa6872`
- Candidate SHA: `e72f4974ea597aa48d2725b131d13e39bcfa6872`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `segment-index-get-multisegment-cold:getHitAsyncJoin` | `41.881 ops/s` | `37.621 ops/s` | `-10.17%` | `worse` |
| `segment-index-get-multisegment-cold:getHitSync` | `48.746 ops/s` | `45.985 ops/s` | `-5.66%` | `warning` |
| `segment-index-get-multisegment-cold:getMissAsyncJoin` | `182927.876 ops/s` | `187149.390 ops/s` | `+2.31%` | `neutral` |
| `segment-index-get-multisegment-cold:getMissSync` | `3855818.743 ops/s` | `3816438.273 ops/s` | `-1.02%` | `neutral` |
| `segment-index-get-multisegment-hot:getHitAsyncJoin` | `94.676 ops/s` | `93.788 ops/s` | `-0.94%` | `neutral` |
| `segment-index-get-multisegment-hot:getHitSync` | `90.465 ops/s` | `86.425 ops/s` | `-4.47%` | `warning` |
| `segment-index-get-multisegment-hot:getMissAsyncJoin` | `186762.518 ops/s` | `188048.034 ops/s` | `+0.69%` | `neutral` |
| `segment-index-get-multisegment-hot:getMissSync` | `3603645.484 ops/s` | `3630861.044 ops/s` | `+0.76%` | `neutral` |
| `segment-index-get-overlay:getHitAsyncJoin` | `186359.988 ops/s` | `187027.690 ops/s` | `+0.36%` | `neutral` |
| `segment-index-get-overlay:getHitSync` | `3562966.262 ops/s` | `3444735.169 ops/s` | `-3.32%` | `warning` |
| `segment-index-get-overlay:getMissAsyncJoin` | `181404.832 ops/s` | `186588.967 ops/s` | `+2.86%` | `neutral` |
| `segment-index-get-overlay:getMissSync` | `3696564.073 ops/s` | `3629930.660 ops/s` | `-1.80%` | `neutral` |
| `segment-index-get-persisted:getHitAsyncJoin` | `65373.215 ops/s` | `64043.475 ops/s` | `-2.03%` | `neutral` |
| `segment-index-get-persisted:getHitSync` | `114057.850 ops/s` | `114598.841 ops/s` | `+0.47%` | `neutral` |
| `segment-index-get-persisted:getMissAsyncJoin` | `185029.025 ops/s` | `181345.196 ops/s` | `-1.99%` | `neutral` |
| `segment-index-get-persisted:getMissSync` | `3723573.651 ops/s` | `3675529.878 ops/s` | `-1.29%` | `neutral` |
| `segment-index-hot-partition-put:putHotPartition` | `3018361.963 ops/s` | `2805138.006 ops/s` | `-7.06%` | `worse` |
| `segment-index-hot-partition-put:putThenGetHotPartition` | `1435504.963 ops/s` | `1657718.424 ops/s` | `+15.48%` | `better` |
| `segment-index-lifecycle:openAndCheckAndRepairConsistency` | `276.828 ms/op` | `278.020 ms/op` | `+0.43%` | `neutral` |
| `segment-index-lifecycle:openAndCompact` | `300.373 ms/op` | `299.008 ms/op` | `-0.45%` | `neutral` |
| `segment-index-lifecycle:openExisting` | `275.362 ms/op` | `277.081 ms/op` | `+0.62%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed` | `516930.227 ops/s` | `498963.295 ops/s` | `-3.48%` | `warning` |
| `segment-index-mixed-drain:partitionedIngestMixed:getWorkload` | `511544.828 ops/s` | `493642.804 ops/s` | `-3.50%` | `warning` |
| `segment-index-mixed-drain:partitionedIngestMixed:putWorkload` | `5385.398 ops/s` | `5320.491 ops/s` | `-1.21%` | `neutral` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed` | `267174.860 ops/s` | `-` | `-` | `removed` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:getWorkload` | `265731.890 ops/s` | `-` | `-` | `removed` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:putWorkload` | `1442.970 ops/s` | `-` | `-` | `removed` |
| `segment-index-persisted-mutation:deleteAsyncJoin` | `2068.826 ops/s` | `2539.384 ops/s` | `+22.75%` | `better` |
| `segment-index-persisted-mutation:deleteSync` | `2749.285 ops/s` | `2738.801 ops/s` | `-0.38%` | `neutral` |
| `segment-index-persisted-mutation:putAsyncJoin` | `2495.975 ops/s` | `2524.955 ops/s` | `+1.16%` | `neutral` |
| `segment-index-persisted-mutation:putSync` | `2700.856 ops/s` | `2745.152 ops/s` | `+1.64%` | `neutral` |
| `single-chunk-entry-write-compact:writeEntrySteadyState` | `8320326.925 ops/s` | `8300281.356 ops/s` | `-0.24%` | `neutral` |
| `single-chunk-entry-write-large:writeEntrySteadyState` | `7772215.136 ops/s` | `7762600.127 ops/s` | `-0.12%` | `neutral` |
| `sorted-data-diff-key-read-compact:readNextKey` | `8523549.655 ops/s` | `8196610.654 ops/s` | `-3.84%` | `warning` |
| `sorted-data-diff-key-read-large:readNextKey` | `6765007.240 ops/s` | `6730413.944 ops/s` | `-0.51%` | `neutral` |
