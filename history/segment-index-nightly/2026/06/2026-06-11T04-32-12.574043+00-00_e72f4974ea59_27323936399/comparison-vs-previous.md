# Benchmark Comparison

- Candidate labels rerun 3x total and median-merged because the initial regression exceeded threshold with high relative score error: `segment-index-get-multisegment-cold,segment-index-get-multisegment-hot`


- Profile: `segment-index-nightly`
- Baseline SHA: `e72f4974ea597aa48d2725b131d13e39bcfa6872`
- Candidate SHA: `e72f4974ea597aa48d2725b131d13e39bcfa6872`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `segment-index-get-multisegment-cold:getHitAsyncJoin` | `48.239 ops/s` | `43.311 ops/s` | `-10.22%` | `worse` |
| `segment-index-get-multisegment-cold:getHitSync` | `48.211 ops/s` | `48.741 ops/s` | `+1.10%` | `neutral` |
| `segment-index-get-multisegment-cold:getMissAsyncJoin` | `170588.728 ops/s` | `184417.302 ops/s` | `+8.11%` | `better` |
| `segment-index-get-multisegment-cold:getMissSync` | `3440505.341 ops/s` | `3709568.689 ops/s` | `+7.82%` | `better` |
| `segment-index-get-multisegment-hot:getHitAsyncJoin` | `89.401 ops/s` | `89.527 ops/s` | `+0.14%` | `neutral` |
| `segment-index-get-multisegment-hot:getHitSync` | `81.308 ops/s` | `93.810 ops/s` | `+15.38%` | `better` |
| `segment-index-get-multisegment-hot:getMissAsyncJoin` | `170900.228 ops/s` | `185629.693 ops/s` | `+8.62%` | `better` |
| `segment-index-get-multisegment-hot:getMissSync` | `3773860.600 ops/s` | `3636859.668 ops/s` | `-3.63%` | `warning` |
| `segment-index-get-overlay:getHitAsyncJoin` | `161652.025 ops/s` | `186039.879 ops/s` | `+15.09%` | `better` |
| `segment-index-get-overlay:getHitSync` | `3942425.109 ops/s` | `3584487.363 ops/s` | `-9.08%` | `worse` |
| `segment-index-get-overlay:getMissAsyncJoin` | `162420.144 ops/s` | `185926.986 ops/s` | `+14.47%` | `better` |
| `segment-index-get-overlay:getMissSync` | `3791248.716 ops/s` | `3710089.797 ops/s` | `-2.14%` | `neutral` |
| `segment-index-get-persisted:getHitAsyncJoin` | `58289.388 ops/s` | `63268.517 ops/s` | `+8.54%` | `better` |
| `segment-index-get-persisted:getHitSync` | `112651.746 ops/s` | `111539.595 ops/s` | `-0.99%` | `neutral` |
| `segment-index-get-persisted:getMissAsyncJoin` | `163855.031 ops/s` | `187135.304 ops/s` | `+14.21%` | `better` |
| `segment-index-get-persisted:getMissSync` | `3680590.953 ops/s` | `3681203.386 ops/s` | `+0.02%` | `neutral` |
| `segment-index-hot-partition-put:putHotPartition` | `2972182.750 ops/s` | `2807990.089 ops/s` | `-5.52%` | `warning` |
| `segment-index-hot-partition-put:putThenGetHotPartition` | `1549335.691 ops/s` | `1543797.335 ops/s` | `-0.36%` | `neutral` |
| `segment-index-lifecycle:openAndCheckAndRepairConsistency` | `243.581 ms/op` | `278.914 ms/op` | `+14.51%` | `better` |
| `segment-index-lifecycle:openAndCompact` | `264.236 ms/op` | `304.566 ms/op` | `+15.26%` | `better` |
| `segment-index-lifecycle:openExisting` | `240.020 ms/op` | `276.643 ms/op` | `+15.26%` | `better` |
| `segment-index-mixed-drain:partitionedIngestMixed` | `518649.353 ops/s` | `504378.377 ops/s` | `-2.75%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed:getWorkload` | `513296.158 ops/s` | `499022.860 ops/s` | `-2.78%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed:putWorkload` | `5353.195 ops/s` | `5355.518 ops/s` | `+0.04%` | `neutral` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed` | `257764.132 ops/s` | `302040.680 ops/s` | `+17.18%` | `better` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:getWorkload` | `250222.915 ops/s` | `266593.761 ops/s` | `+6.54%` | `better` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:putWorkload` | `7541.218 ops/s` | `35446.920 ops/s` | `+370.04%` | `better` |
| `segment-index-persisted-mutation:deleteAsyncJoin` | `2028.192 ops/s` | `2502.572 ops/s` | `+23.39%` | `better` |
| `segment-index-persisted-mutation:deleteSync` | `2279.408 ops/s` | `2778.528 ops/s` | `+21.90%` | `better` |
| `segment-index-persisted-mutation:putAsyncJoin` | `2016.157 ops/s` | `2508.340 ops/s` | `+24.41%` | `better` |
| `segment-index-persisted-mutation:putSync` | `2241.161 ops/s` | `2644.111 ops/s` | `+17.98%` | `better` |
| `single-chunk-entry-write-compact:writeEntrySteadyState` | `8458660.006 ops/s` | `8285968.415 ops/s` | `-2.04%` | `neutral` |
| `single-chunk-entry-write-large:writeEntrySteadyState` | `7867101.302 ops/s` | `7370021.537 ops/s` | `-6.32%` | `warning` |
| `sorted-data-diff-key-read-compact:readNextKey` | `9488783.183 ops/s` | `7820170.337 ops/s` | `-17.59%` | `worse` |
| `sorted-data-diff-key-read-large:readNextKey` | `7415244.473 ops/s` | `6681804.388 ops/s` | `-9.89%` | `worse` |
