# Benchmark Comparison

- Candidate labels rerun 3x total and median-merged because the initial regression exceeded threshold with high relative score error: `segment-index-get-multisegment-cold,segment-index-get-multisegment-hot`


- Profile: `segment-index-nightly`
- Baseline SHA: `e72f4974ea597aa48d2725b131d13e39bcfa6872`
- Candidate SHA: `e72f4974ea597aa48d2725b131d13e39bcfa6872`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `segment-index-get-multisegment-cold:getHitAsyncJoin` | `42.731 ops/s` | `50.247 ops/s` | `+17.59%` | `better` |
| `segment-index-get-multisegment-cold:getHitSync` | `43.654 ops/s` | `44.434 ops/s` | `+1.79%` | `neutral` |
| `segment-index-get-multisegment-cold:getMissAsyncJoin` | `172825.766 ops/s` | `169636.785 ops/s` | `-1.85%` | `neutral` |
| `segment-index-get-multisegment-cold:getMissSync` | `3866871.864 ops/s` | `3796589.179 ops/s` | `-1.82%` | `neutral` |
| `segment-index-get-multisegment-hot:getHitAsyncJoin` | `111.368 ops/s` | `89.738 ops/s` | `-19.42%` | `worse` |
| `segment-index-get-multisegment-hot:getHitSync` | `91.730 ops/s` | `85.705 ops/s` | `-6.57%` | `warning` |
| `segment-index-get-multisegment-hot:getMissAsyncJoin` | `174292.229 ops/s` | `171049.321 ops/s` | `-1.86%` | `neutral` |
| `segment-index-get-multisegment-hot:getMissSync` | `4033291.669 ops/s` | `3728102.039 ops/s` | `-7.57%` | `worse` |
| `segment-index-get-overlay:getHitAsyncJoin` | `167423.247 ops/s` | `159600.013 ops/s` | `-4.67%` | `warning` |
| `segment-index-get-overlay:getHitSync` | `4005659.731 ops/s` | `4186443.326 ops/s` | `+4.51%` | `better` |
| `segment-index-get-overlay:getMissAsyncJoin` | `160014.204 ops/s` | `162904.777 ops/s` | `+1.81%` | `neutral` |
| `segment-index-get-overlay:getMissSync` | `3589694.947 ops/s` | `3807759.122 ops/s` | `+6.07%` | `better` |
| `segment-index-get-persisted:getHitAsyncJoin` | `59382.416 ops/s` | `59536.201 ops/s` | `+0.26%` | `neutral` |
| `segment-index-get-persisted:getHitSync` | `117210.808 ops/s` | `111303.888 ops/s` | `-5.04%` | `warning` |
| `segment-index-get-persisted:getMissAsyncJoin` | `162256.601 ops/s` | `152446.462 ops/s` | `-6.05%` | `warning` |
| `segment-index-get-persisted:getMissSync` | `3599526.147 ops/s` | `3793628.601 ops/s` | `+5.39%` | `better` |
| `segment-index-hot-partition-put:putHotPartition` | `2764822.764 ops/s` | `2659059.530 ops/s` | `-3.83%` | `warning` |
| `segment-index-hot-partition-put:putThenGetHotPartition` | `1598470.017 ops/s` | `1516908.814 ops/s` | `-5.10%` | `warning` |
| `segment-index-lifecycle:openAndCheckAndRepairConsistency` | `244.800 ms/op` | `249.260 ms/op` | `+1.82%` | `neutral` |
| `segment-index-lifecycle:openAndCompact` | `265.766 ms/op` | `269.599 ms/op` | `+1.44%` | `neutral` |
| `segment-index-lifecycle:openExisting` | `240.933 ms/op` | `245.529 ms/op` | `+1.91%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed` | `506189.097 ops/s` | `487768.294 ops/s` | `-3.64%` | `warning` |
| `segment-index-mixed-drain:partitionedIngestMixed:getWorkload` | `500841.249 ops/s` | `482435.602 ops/s` | `-3.67%` | `warning` |
| `segment-index-mixed-drain:partitionedIngestMixed:putWorkload` | `5347.847 ops/s` | `5332.692 ops/s` | `-0.28%` | `neutral` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed` | `262677.107 ops/s` | `300878.375 ops/s` | `+14.54%` | `better` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:getWorkload` | `261322.263 ops/s` | `263692.800 ops/s` | `+0.91%` | `neutral` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:putWorkload` | `1354.844 ops/s` | `37185.575 ops/s` | `+2644.64%` | `better` |
| `segment-index-persisted-mutation:deleteAsyncJoin` | `1995.375 ops/s` | `2019.164 ops/s` | `+1.19%` | `neutral` |
| `segment-index-persisted-mutation:deleteSync` | `2186.491 ops/s` | `2197.176 ops/s` | `+0.49%` | `neutral` |
| `segment-index-persisted-mutation:putAsyncJoin` | `1962.279 ops/s` | `2007.446 ops/s` | `+2.30%` | `neutral` |
| `segment-index-persisted-mutation:putSync` | `2072.412 ops/s` | `2168.155 ops/s` | `+4.62%` | `better` |
| `single-chunk-entry-write-compact:writeEntrySteadyState` | `8506531.538 ops/s` | `8502717.670 ops/s` | `-0.04%` | `neutral` |
| `single-chunk-entry-write-large:writeEntrySteadyState` | `7940126.533 ops/s` | `7945907.548 ops/s` | `+0.07%` | `neutral` |
| `sorted-data-diff-key-read-compact:readNextKey` | `9471661.929 ops/s` | `8845214.510 ops/s` | `-6.61%` | `warning` |
| `sorted-data-diff-key-read-large:readNextKey` | `7349591.111 ops/s` | `7382179.673 ops/s` | `+0.44%` | `neutral` |
