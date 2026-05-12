# Benchmark Comparison

- Candidate labels rerun 3x total and median-merged because the initial regression exceeded threshold with high relative score error: `segment-index-get-multisegment-cold`


- Profile: `segment-index-nightly`
- Baseline SHA: `e72f4974ea597aa48d2725b131d13e39bcfa6872`
- Candidate SHA: `e72f4974ea597aa48d2725b131d13e39bcfa6872`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `segment-index-get-multisegment-cold:getHitAsyncJoin` | `46.020 ops/s` | `41.881 ops/s` | `-8.99%` | `worse` |
| `segment-index-get-multisegment-cold:getHitSync` | `48.534 ops/s` | `48.746 ops/s` | `+0.44%` | `neutral` |
| `segment-index-get-multisegment-cold:getMissAsyncJoin` | `170559.273 ops/s` | `182927.876 ops/s` | `+7.25%` | `better` |
| `segment-index-get-multisegment-cold:getMissSync` | `3409275.893 ops/s` | `3855818.743 ops/s` | `+13.10%` | `better` |
| `segment-index-get-multisegment-hot:getHitAsyncJoin` | `85.659 ops/s` | `94.676 ops/s` | `+10.53%` | `better` |
| `segment-index-get-multisegment-hot:getHitSync` | `85.432 ops/s` | `90.465 ops/s` | `+5.89%` | `better` |
| `segment-index-get-multisegment-hot:getMissAsyncJoin` | `171762.257 ops/s` | `186762.518 ops/s` | `+8.73%` | `better` |
| `segment-index-get-multisegment-hot:getMissSync` | `3702433.533 ops/s` | `3603645.484 ops/s` | `-2.67%` | `neutral` |
| `segment-index-get-overlay:getHitAsyncJoin` | `160906.402 ops/s` | `186359.988 ops/s` | `+15.82%` | `better` |
| `segment-index-get-overlay:getHitSync` | `4316645.484 ops/s` | `3562966.262 ops/s` | `-17.46%` | `worse` |
| `segment-index-get-overlay:getMissAsyncJoin` | `164524.113 ops/s` | `181404.832 ops/s` | `+10.26%` | `better` |
| `segment-index-get-overlay:getMissSync` | `3876995.722 ops/s` | `3696564.073 ops/s` | `-4.65%` | `warning` |
| `segment-index-get-persisted:getHitAsyncJoin` | `61040.115 ops/s` | `65373.215 ops/s` | `+7.10%` | `better` |
| `segment-index-get-persisted:getHitSync` | `117449.237 ops/s` | `114057.850 ops/s` | `-2.89%` | `neutral` |
| `segment-index-get-persisted:getMissAsyncJoin` | `165043.257 ops/s` | `185029.025 ops/s` | `+12.11%` | `better` |
| `segment-index-get-persisted:getMissSync` | `3644350.438 ops/s` | `3723573.651 ops/s` | `+2.17%` | `neutral` |
| `segment-index-hot-partition-put:putHotPartition` | `2878623.005 ops/s` | `3018361.963 ops/s` | `+4.85%` | `better` |
| `segment-index-hot-partition-put:putThenGetHotPartition` | `1651247.918 ops/s` | `1435504.963 ops/s` | `-13.07%` | `worse` |
| `segment-index-lifecycle:openAndCheckAndRepairConsistency` | `247.774 ms/op` | `276.828 ms/op` | `+11.73%` | `better` |
| `segment-index-lifecycle:openAndCompact` | `268.843 ms/op` | `300.373 ms/op` | `+11.73%` | `better` |
| `segment-index-lifecycle:openExisting` | `245.339 ms/op` | `275.362 ms/op` | `+12.24%` | `better` |
| `segment-index-mixed-drain:partitionedIngestMixed` | `505009.501 ops/s` | `516930.227 ops/s` | `+2.36%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed:getWorkload` | `499740.809 ops/s` | `511544.828 ops/s` | `+2.36%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed:putWorkload` | `5268.692 ops/s` | `5385.398 ops/s` | `+2.22%` | `neutral` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed` | `260190.301 ops/s` | `267174.860 ops/s` | `+2.68%` | `neutral` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:getWorkload` | `258846.322 ops/s` | `265731.890 ops/s` | `+2.66%` | `neutral` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:putWorkload` | `1343.979 ops/s` | `1442.970 ops/s` | `+7.37%` | `better` |
| `segment-index-persisted-mutation:deleteAsyncJoin` | `1966.338 ops/s` | `2068.826 ops/s` | `+5.21%` | `better` |
| `segment-index-persisted-mutation:deleteSync` | `2110.536 ops/s` | `2749.285 ops/s` | `+30.26%` | `better` |
| `segment-index-persisted-mutation:putAsyncJoin` | `1979.715 ops/s` | `2495.975 ops/s` | `+26.08%` | `better` |
| `segment-index-persisted-mutation:putSync` | `2110.956 ops/s` | `2700.856 ops/s` | `+27.94%` | `better` |
| `single-chunk-entry-write-compact:writeEntrySteadyState` | `8457547.645 ops/s` | `8320326.925 ops/s` | `-1.62%` | `neutral` |
| `single-chunk-entry-write-large:writeEntrySteadyState` | `7911274.451 ops/s` | `7772215.136 ops/s` | `-1.76%` | `neutral` |
| `sorted-data-diff-key-read-compact:readNextKey` | `9141101.338 ops/s` | `8523549.655 ops/s` | `-6.76%` | `warning` |
| `sorted-data-diff-key-read-large:readNextKey` | `7068451.107 ops/s` | `6765007.240 ops/s` | `-4.29%` | `warning` |
