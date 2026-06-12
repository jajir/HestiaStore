# Benchmark Comparison

- Candidate labels rerun 3x total and median-merged because the initial regression exceeded threshold with high relative score error: `segment-index-get-multisegment-cold,segment-index-mixed-split-heavy`


- Profile: `segment-index-nightly`
- Baseline SHA: `e72f4974ea597aa48d2725b131d13e39bcfa6872`
- Candidate SHA: `e72f4974ea597aa48d2725b131d13e39bcfa6872`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `segment-index-get-multisegment-cold:getHitAsyncJoin` | `43.311 ops/s` | `38.814 ops/s` | `-10.38%` | `worse` |
| `segment-index-get-multisegment-cold:getHitSync` | `48.741 ops/s` | `40.399 ops/s` | `-17.11%` | `worse` |
| `segment-index-get-multisegment-cold:getMissAsyncJoin` | `184417.302 ops/s` | `186029.169 ops/s` | `+0.87%` | `neutral` |
| `segment-index-get-multisegment-cold:getMissSync` | `3709568.689 ops/s` | `3504545.933 ops/s` | `-5.53%` | `warning` |
| `segment-index-get-multisegment-hot:getHitAsyncJoin` | `89.527 ops/s` | `93.416 ops/s` | `+4.34%` | `better` |
| `segment-index-get-multisegment-hot:getHitSync` | `93.810 ops/s` | `95.856 ops/s` | `+2.18%` | `neutral` |
| `segment-index-get-multisegment-hot:getMissAsyncJoin` | `185629.693 ops/s` | `187159.515 ops/s` | `+0.82%` | `neutral` |
| `segment-index-get-multisegment-hot:getMissSync` | `3636859.668 ops/s` | `3623869.180 ops/s` | `-0.36%` | `neutral` |
| `segment-index-get-overlay:getHitAsyncJoin` | `186039.879 ops/s` | `181529.298 ops/s` | `-2.42%` | `neutral` |
| `segment-index-get-overlay:getHitSync` | `3584487.363 ops/s` | `3564540.232 ops/s` | `-0.56%` | `neutral` |
| `segment-index-get-overlay:getMissAsyncJoin` | `185926.986 ops/s` | `181353.683 ops/s` | `-2.46%` | `neutral` |
| `segment-index-get-overlay:getMissSync` | `3710089.797 ops/s` | `3482412.461 ops/s` | `-6.14%` | `warning` |
| `segment-index-get-persisted:getHitAsyncJoin` | `63268.517 ops/s` | `62768.346 ops/s` | `-0.79%` | `neutral` |
| `segment-index-get-persisted:getHitSync` | `111539.595 ops/s` | `114967.442 ops/s` | `+3.07%` | `better` |
| `segment-index-get-persisted:getMissAsyncJoin` | `187135.304 ops/s` | `177727.653 ops/s` | `-5.03%` | `warning` |
| `segment-index-get-persisted:getMissSync` | `3681203.386 ops/s` | `3735534.606 ops/s` | `+1.48%` | `neutral` |
| `segment-index-hot-partition-put:putHotPartition` | `2807990.089 ops/s` | `2454250.130 ops/s` | `-12.60%` | `worse` |
| `segment-index-hot-partition-put:putThenGetHotPartition` | `1543797.335 ops/s` | `1602058.645 ops/s` | `+3.77%` | `better` |
| `segment-index-lifecycle:openAndCheckAndRepairConsistency` | `278.914 ms/op` | `278.815 ms/op` | `-0.04%` | `neutral` |
| `segment-index-lifecycle:openAndCompact` | `304.566 ms/op` | `299.354 ms/op` | `-1.71%` | `neutral` |
| `segment-index-lifecycle:openExisting` | `276.643 ms/op` | `274.675 ms/op` | `-0.71%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed` | `504378.377 ops/s` | `554421.208 ops/s` | `+9.92%` | `better` |
| `segment-index-mixed-drain:partitionedIngestMixed:getWorkload` | `499022.860 ops/s` | `549039.261 ops/s` | `+10.02%` | `better` |
| `segment-index-mixed-drain:partitionedIngestMixed:putWorkload` | `5355.518 ops/s` | `5381.946 ops/s` | `+0.49%` | `neutral` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed` | `302040.680 ops/s` | `266046.779 ops/s` | `-11.92%` | `worse` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:getWorkload` | `266593.761 ops/s` | `263663.070 ops/s` | `-1.10%` | `neutral` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:putWorkload` | `35446.920 ops/s` | `1641.728 ops/s` | `-95.37%` | `worse` |
| `segment-index-persisted-mutation:deleteAsyncJoin` | `2502.572 ops/s` | `2535.359 ops/s` | `+1.31%` | `neutral` |
| `segment-index-persisted-mutation:deleteSync` | `2778.528 ops/s` | `2785.292 ops/s` | `+0.24%` | `neutral` |
| `segment-index-persisted-mutation:putAsyncJoin` | `2508.340 ops/s` | `2512.749 ops/s` | `+0.18%` | `neutral` |
| `segment-index-persisted-mutation:putSync` | `2644.111 ops/s` | `2763.398 ops/s` | `+4.51%` | `better` |
| `single-chunk-entry-write-compact:writeEntrySteadyState` | `8285968.415 ops/s` | `8154453.509 ops/s` | `-1.59%` | `neutral` |
| `single-chunk-entry-write-large:writeEntrySteadyState` | `7370021.537 ops/s` | `7800086.831 ops/s` | `+5.84%` | `better` |
| `sorted-data-diff-key-read-compact:readNextKey` | `7820170.337 ops/s` | `8642022.262 ops/s` | `+10.51%` | `better` |
| `sorted-data-diff-key-read-large:readNextKey` | `6681804.388 ops/s` | `6631707.820 ops/s` | `-0.75%` | `neutral` |
