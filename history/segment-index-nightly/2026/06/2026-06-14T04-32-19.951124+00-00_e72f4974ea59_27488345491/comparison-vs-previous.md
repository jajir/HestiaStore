# Benchmark Comparison

- Candidate labels rerun 3x total and median-merged because the initial regression exceeded threshold with high relative score error: `segment-index-get-multisegment-cold,segment-index-get-multisegment-hot`


- Profile: `segment-index-nightly`
- Baseline SHA: `e72f4974ea597aa48d2725b131d13e39bcfa6872`
- Candidate SHA: `e72f4974ea597aa48d2725b131d13e39bcfa6872`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `segment-index-get-multisegment-cold:getHitAsyncJoin` | `48.709 ops/s` | `43.087 ops/s` | `-11.54%` | `worse` |
| `segment-index-get-multisegment-cold:getHitSync` | `48.367 ops/s` | `39.211 ops/s` | `-18.93%` | `worse` |
| `segment-index-get-multisegment-cold:getMissAsyncJoin` | `184703.808 ops/s` | `185525.900 ops/s` | `+0.45%` | `neutral` |
| `segment-index-get-multisegment-cold:getMissSync` | `3784618.860 ops/s` | `3623408.578 ops/s` | `-4.26%` | `warning` |
| `segment-index-get-multisegment-hot:getHitAsyncJoin` | `98.063 ops/s` | `85.922 ops/s` | `-12.38%` | `worse` |
| `segment-index-get-multisegment-hot:getHitSync` | `90.259 ops/s` | `85.686 ops/s` | `-5.07%` | `warning` |
| `segment-index-get-multisegment-hot:getMissAsyncJoin` | `182133.838 ops/s` | `184619.837 ops/s` | `+1.36%` | `neutral` |
| `segment-index-get-multisegment-hot:getMissSync` | `3479112.942 ops/s` | `3598893.047 ops/s` | `+3.44%` | `better` |
| `segment-index-get-overlay:getHitAsyncJoin` | `186916.510 ops/s` | `184212.480 ops/s` | `-1.45%` | `neutral` |
| `segment-index-get-overlay:getHitSync` | `4150482.767 ops/s` | `4055640.095 ops/s` | `-2.29%` | `neutral` |
| `segment-index-get-overlay:getMissAsyncJoin` | `186068.242 ops/s` | `186817.822 ops/s` | `+0.40%` | `neutral` |
| `segment-index-get-overlay:getMissSync` | `3542867.756 ops/s` | `3726165.756 ops/s` | `+5.17%` | `better` |
| `segment-index-get-persisted:getHitAsyncJoin` | `64099.544 ops/s` | `62648.687 ops/s` | `-2.26%` | `neutral` |
| `segment-index-get-persisted:getHitSync` | `112380.953 ops/s` | `115457.912 ops/s` | `+2.74%` | `neutral` |
| `segment-index-get-persisted:getMissAsyncJoin` | `180765.013 ops/s` | `180228.875 ops/s` | `-0.30%` | `neutral` |
| `segment-index-get-persisted:getMissSync` | `3408270.168 ops/s` | `4001402.117 ops/s` | `+17.40%` | `better` |
| `segment-index-hot-partition-put:putHotPartition` | `2595833.042 ops/s` | `2702436.651 ops/s` | `+4.11%` | `better` |
| `segment-index-hot-partition-put:putThenGetHotPartition` | `1333500.418 ops/s` | `1635957.076 ops/s` | `+22.68%` | `better` |
| `segment-index-lifecycle:openAndCheckAndRepairConsistency` | `275.229 ms/op` | `280.198 ms/op` | `+1.81%` | `neutral` |
| `segment-index-lifecycle:openAndCompact` | `300.401 ms/op` | `296.685 ms/op` | `-1.24%` | `neutral` |
| `segment-index-lifecycle:openExisting` | `274.189 ms/op` | `274.478 ms/op` | `+0.11%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed` | `509357.929 ops/s` | `506311.914 ops/s` | `-0.60%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed:getWorkload` | `504030.882 ops/s` | `500942.603 ops/s` | `-0.61%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed:putWorkload` | `5327.047 ops/s` | `5369.311 ops/s` | `+0.79%` | `neutral` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed` | `271556.809 ops/s` | `259241.499 ops/s` | `-4.54%` | `warning` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:getWorkload` | `270215.464 ops/s` | `257786.533 ops/s` | `-4.60%` | `warning` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:putWorkload` | `1341.345 ops/s` | `1454.965 ops/s` | `+8.47%` | `better` |
| `segment-index-persisted-mutation:deleteAsyncJoin` | `2518.362 ops/s` | `2517.741 ops/s` | `-0.02%` | `neutral` |
| `segment-index-persisted-mutation:deleteSync` | `2760.587 ops/s` | `2761.434 ops/s` | `+0.03%` | `neutral` |
| `segment-index-persisted-mutation:putAsyncJoin` | `2467.072 ops/s` | `2504.159 ops/s` | `+1.50%` | `neutral` |
| `segment-index-persisted-mutation:putSync` | `2673.547 ops/s` | `2749.537 ops/s` | `+2.84%` | `neutral` |
| `single-chunk-entry-write-compact:writeEntrySteadyState` | `8266611.615 ops/s` | `8159361.641 ops/s` | `-1.30%` | `neutral` |
| `single-chunk-entry-write-large:writeEntrySteadyState` | `7750315.111 ops/s` | `7812013.295 ops/s` | `+0.80%` | `neutral` |
| `sorted-data-diff-key-read-compact:readNextKey` | `8527670.839 ops/s` | `8615904.811 ops/s` | `+1.03%` | `neutral` |
| `sorted-data-diff-key-read-large:readNextKey` | `6781241.488 ops/s` | `6778225.429 ops/s` | `-0.04%` | `neutral` |
