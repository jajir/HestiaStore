# Benchmark Comparison

- Candidate labels rerun 3x total and median-merged because the initial regression exceeded threshold with high relative score error: `segment-index-get-persisted`


- Profile: `segment-index-nightly`
- Baseline SHA: `e72f4974ea597aa48d2725b131d13e39bcfa6872`
- Candidate SHA: `e72f4974ea597aa48d2725b131d13e39bcfa6872`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `segment-index-get-multisegment-cold:getHitAsyncJoin` | `38.814 ops/s` | `48.709 ops/s` | `+25.49%` | `better` |
| `segment-index-get-multisegment-cold:getHitSync` | `40.399 ops/s` | `48.367 ops/s` | `+19.72%` | `better` |
| `segment-index-get-multisegment-cold:getMissAsyncJoin` | `186029.169 ops/s` | `184703.808 ops/s` | `-0.71%` | `neutral` |
| `segment-index-get-multisegment-cold:getMissSync` | `3504545.933 ops/s` | `3784618.860 ops/s` | `+7.99%` | `better` |
| `segment-index-get-multisegment-hot:getHitAsyncJoin` | `93.416 ops/s` | `98.063 ops/s` | `+4.98%` | `better` |
| `segment-index-get-multisegment-hot:getHitSync` | `95.856 ops/s` | `90.259 ops/s` | `-5.84%` | `warning` |
| `segment-index-get-multisegment-hot:getMissAsyncJoin` | `187159.515 ops/s` | `182133.838 ops/s` | `-2.69%` | `neutral` |
| `segment-index-get-multisegment-hot:getMissSync` | `3623869.180 ops/s` | `3479112.942 ops/s` | `-3.99%` | `warning` |
| `segment-index-get-overlay:getHitAsyncJoin` | `181529.298 ops/s` | `186916.510 ops/s` | `+2.97%` | `neutral` |
| `segment-index-get-overlay:getHitSync` | `3564540.232 ops/s` | `4150482.767 ops/s` | `+16.44%` | `better` |
| `segment-index-get-overlay:getMissAsyncJoin` | `181353.683 ops/s` | `186068.242 ops/s` | `+2.60%` | `neutral` |
| `segment-index-get-overlay:getMissSync` | `3482412.461 ops/s` | `3542867.756 ops/s` | `+1.74%` | `neutral` |
| `segment-index-get-persisted:getHitAsyncJoin` | `62768.346 ops/s` | `64099.544 ops/s` | `+2.12%` | `neutral` |
| `segment-index-get-persisted:getHitSync` | `114967.442 ops/s` | `112380.953 ops/s` | `-2.25%` | `neutral` |
| `segment-index-get-persisted:getMissAsyncJoin` | `177727.653 ops/s` | `180765.013 ops/s` | `+1.71%` | `neutral` |
| `segment-index-get-persisted:getMissSync` | `3735534.606 ops/s` | `3408270.168 ops/s` | `-8.76%` | `worse` |
| `segment-index-hot-partition-put:putHotPartition` | `2454250.130 ops/s` | `2595833.042 ops/s` | `+5.77%` | `better` |
| `segment-index-hot-partition-put:putThenGetHotPartition` | `1602058.645 ops/s` | `1333500.418 ops/s` | `-16.76%` | `worse` |
| `segment-index-lifecycle:openAndCheckAndRepairConsistency` | `278.815 ms/op` | `275.229 ms/op` | `-1.29%` | `neutral` |
| `segment-index-lifecycle:openAndCompact` | `299.354 ms/op` | `300.401 ms/op` | `+0.35%` | `neutral` |
| `segment-index-lifecycle:openExisting` | `274.675 ms/op` | `274.189 ms/op` | `-0.18%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed` | `554421.208 ops/s` | `509357.929 ops/s` | `-8.13%` | `worse` |
| `segment-index-mixed-drain:partitionedIngestMixed:getWorkload` | `549039.261 ops/s` | `504030.882 ops/s` | `-8.20%` | `worse` |
| `segment-index-mixed-drain:partitionedIngestMixed:putWorkload` | `5381.946 ops/s` | `5327.047 ops/s` | `-1.02%` | `neutral` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed` | `266046.779 ops/s` | `271556.809 ops/s` | `+2.07%` | `neutral` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:getWorkload` | `263663.070 ops/s` | `270215.464 ops/s` | `+2.49%` | `neutral` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:putWorkload` | `1641.728 ops/s` | `1341.345 ops/s` | `-18.30%` | `worse` |
| `segment-index-persisted-mutation:deleteAsyncJoin` | `2535.359 ops/s` | `2518.362 ops/s` | `-0.67%` | `neutral` |
| `segment-index-persisted-mutation:deleteSync` | `2785.292 ops/s` | `2760.587 ops/s` | `-0.89%` | `neutral` |
| `segment-index-persisted-mutation:putAsyncJoin` | `2512.749 ops/s` | `2467.072 ops/s` | `-1.82%` | `neutral` |
| `segment-index-persisted-mutation:putSync` | `2763.398 ops/s` | `2673.547 ops/s` | `-3.25%` | `warning` |
| `single-chunk-entry-write-compact:writeEntrySteadyState` | `8154453.509 ops/s` | `8266611.615 ops/s` | `+1.38%` | `neutral` |
| `single-chunk-entry-write-large:writeEntrySteadyState` | `7800086.831 ops/s` | `7750315.111 ops/s` | `-0.64%` | `neutral` |
| `sorted-data-diff-key-read-compact:readNextKey` | `8642022.262 ops/s` | `8527670.839 ops/s` | `-1.32%` | `neutral` |
| `sorted-data-diff-key-read-large:readNextKey` | `6631707.820 ops/s` | `6781241.488 ops/s` | `+2.25%` | `neutral` |
