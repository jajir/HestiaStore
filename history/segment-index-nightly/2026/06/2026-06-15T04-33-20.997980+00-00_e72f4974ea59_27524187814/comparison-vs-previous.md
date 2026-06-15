# Benchmark Comparison

- Profile: `segment-index-nightly`
- Baseline SHA: `e72f4974ea597aa48d2725b131d13e39bcfa6872`
- Candidate SHA: `e72f4974ea597aa48d2725b131d13e39bcfa6872`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `segment-index-get-multisegment-cold:getHitAsyncJoin` | `43.087 ops/s` | `50.313 ops/s` | `+16.77%` | `better` |
| `segment-index-get-multisegment-cold:getHitSync` | `39.211 ops/s` | `41.827 ops/s` | `+6.67%` | `better` |
| `segment-index-get-multisegment-cold:getMissAsyncJoin` | `185525.900 ops/s` | `184638.280 ops/s` | `-0.48%` | `neutral` |
| `segment-index-get-multisegment-cold:getMissSync` | `3623408.578 ops/s` | `3368826.431 ops/s` | `-7.03%` | `worse` |
| `segment-index-get-multisegment-hot:getHitAsyncJoin` | `85.922 ops/s` | `85.442 ops/s` | `-0.56%` | `neutral` |
| `segment-index-get-multisegment-hot:getHitSync` | `85.686 ops/s` | `88.910 ops/s` | `+3.76%` | `better` |
| `segment-index-get-multisegment-hot:getMissAsyncJoin` | `184619.837 ops/s` | `183948.880 ops/s` | `-0.36%` | `neutral` |
| `segment-index-get-multisegment-hot:getMissSync` | `3598893.047 ops/s` | `3359494.042 ops/s` | `-6.65%` | `warning` |
| `segment-index-get-overlay:getHitAsyncJoin` | `184212.480 ops/s` | `182926.205 ops/s` | `-0.70%` | `neutral` |
| `segment-index-get-overlay:getHitSync` | `4055640.095 ops/s` | `3651023.379 ops/s` | `-9.98%` | `worse` |
| `segment-index-get-overlay:getMissAsyncJoin` | `186817.822 ops/s` | `183403.242 ops/s` | `-1.83%` | `neutral` |
| `segment-index-get-overlay:getMissSync` | `3726165.756 ops/s` | `3634300.663 ops/s` | `-2.47%` | `neutral` |
| `segment-index-get-persisted:getHitAsyncJoin` | `62648.687 ops/s` | `59975.896 ops/s` | `-4.27%` | `warning` |
| `segment-index-get-persisted:getHitSync` | `115457.912 ops/s` | `111627.095 ops/s` | `-3.32%` | `warning` |
| `segment-index-get-persisted:getMissAsyncJoin` | `180228.875 ops/s` | `179253.381 ops/s` | `-0.54%` | `neutral` |
| `segment-index-get-persisted:getMissSync` | `4001402.117 ops/s` | `3666550.553 ops/s` | `-8.37%` | `worse` |
| `segment-index-hot-partition-put:putHotPartition` | `2702436.651 ops/s` | `2928244.976 ops/s` | `+8.36%` | `better` |
| `segment-index-hot-partition-put:putThenGetHotPartition` | `1635957.076 ops/s` | `1497552.137 ops/s` | `-8.46%` | `worse` |
| `segment-index-lifecycle:openAndCheckAndRepairConsistency` | `280.198 ms/op` | `278.349 ms/op` | `-0.66%` | `neutral` |
| `segment-index-lifecycle:openAndCompact` | `296.685 ms/op` | `301.710 ms/op` | `+1.69%` | `neutral` |
| `segment-index-lifecycle:openExisting` | `274.478 ms/op` | `273.406 ms/op` | `-0.39%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed` | `506311.914 ops/s` | `507486.747 ops/s` | `+0.23%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed:getWorkload` | `500942.603 ops/s` | `502193.166 ops/s` | `+0.25%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed:putWorkload` | `5369.311 ops/s` | `5293.580 ops/s` | `-1.41%` | `neutral` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed` | `259241.499 ops/s` | `258170.148 ops/s` | `-0.41%` | `neutral` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:getWorkload` | `257786.533 ops/s` | `256694.315 ops/s` | `-0.42%` | `neutral` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:putWorkload` | `1454.965 ops/s` | `1475.834 ops/s` | `+1.43%` | `neutral` |
| `segment-index-persisted-mutation:deleteAsyncJoin` | `2517.741 ops/s` | `2546.258 ops/s` | `+1.13%` | `neutral` |
| `segment-index-persisted-mutation:deleteSync` | `2761.434 ops/s` | `2807.389 ops/s` | `+1.66%` | `neutral` |
| `segment-index-persisted-mutation:putAsyncJoin` | `2504.159 ops/s` | `2513.389 ops/s` | `+0.37%` | `neutral` |
| `segment-index-persisted-mutation:putSync` | `2749.537 ops/s` | `2775.332 ops/s` | `+0.94%` | `neutral` |
| `single-chunk-entry-write-compact:writeEntrySteadyState` | `8159361.641 ops/s` | `8285100.741 ops/s` | `+1.54%` | `neutral` |
| `single-chunk-entry-write-large:writeEntrySteadyState` | `7812013.295 ops/s` | `7851361.353 ops/s` | `+0.50%` | `neutral` |
| `sorted-data-diff-key-read-compact:readNextKey` | `8615904.811 ops/s` | `8525035.009 ops/s` | `-1.05%` | `neutral` |
| `sorted-data-diff-key-read-large:readNextKey` | `6778225.429 ops/s` | `6710109.783 ops/s` | `-1.00%` | `neutral` |
