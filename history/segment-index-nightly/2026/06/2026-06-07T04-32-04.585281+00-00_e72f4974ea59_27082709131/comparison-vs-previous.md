# Benchmark Comparison

- Candidate labels rerun 3x total and median-merged because the initial regression exceeded threshold with high relative score error: `segment-index-get-multisegment-cold,segment-index-mixed-split-heavy`


- Profile: `segment-index-nightly`
- Baseline SHA: `e72f4974ea597aa48d2725b131d13e39bcfa6872`
- Candidate SHA: `e72f4974ea597aa48d2725b131d13e39bcfa6872`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `segment-index-get-multisegment-cold:getHitAsyncJoin` | `54.898 ops/s` | `41.658 ops/s` | `-24.12%` | `worse` |
| `segment-index-get-multisegment-cold:getHitSync` | `41.187 ops/s` | `36.593 ops/s` | `-11.16%` | `worse` |
| `segment-index-get-multisegment-cold:getMissAsyncJoin` | `184096.292 ops/s` | `184803.049 ops/s` | `+0.38%` | `neutral` |
| `segment-index-get-multisegment-cold:getMissSync` | `3633713.021 ops/s` | `3566520.046 ops/s` | `-1.85%` | `neutral` |
| `segment-index-get-multisegment-hot:getHitAsyncJoin` | `94.636 ops/s` | `96.391 ops/s` | `+1.85%` | `neutral` |
| `segment-index-get-multisegment-hot:getHitSync` | `81.484 ops/s` | `97.812 ops/s` | `+20.04%` | `better` |
| `segment-index-get-multisegment-hot:getMissAsyncJoin` | `182912.192 ops/s` | `183670.382 ops/s` | `+0.41%` | `neutral` |
| `segment-index-get-multisegment-hot:getMissSync` | `3665597.617 ops/s` | `3820156.299 ops/s` | `+4.22%` | `better` |
| `segment-index-get-overlay:getHitAsyncJoin` | `185771.090 ops/s` | `179163.070 ops/s` | `-3.56%` | `warning` |
| `segment-index-get-overlay:getHitSync` | `3690168.288 ops/s` | `3720827.553 ops/s` | `+0.83%` | `neutral` |
| `segment-index-get-overlay:getMissAsyncJoin` | `182802.334 ops/s` | `185213.757 ops/s` | `+1.32%` | `neutral` |
| `segment-index-get-overlay:getMissSync` | `3662356.741 ops/s` | `3881562.090 ops/s` | `+5.99%` | `better` |
| `segment-index-get-persisted:getHitAsyncJoin` | `63125.046 ops/s` | `68086.944 ops/s` | `+7.86%` | `better` |
| `segment-index-get-persisted:getHitSync` | `112857.675 ops/s` | `115640.220 ops/s` | `+2.47%` | `neutral` |
| `segment-index-get-persisted:getMissAsyncJoin` | `177617.013 ops/s` | `183973.882 ops/s` | `+3.58%` | `better` |
| `segment-index-get-persisted:getMissSync` | `3365987.223 ops/s` | `3493071.711 ops/s` | `+3.78%` | `better` |
| `segment-index-hot-partition-put:putHotPartition` | `2353513.792 ops/s` | `2769865.181 ops/s` | `+17.69%` | `better` |
| `segment-index-hot-partition-put:putThenGetHotPartition` | `1355502.169 ops/s` | `1491829.651 ops/s` | `+10.06%` | `better` |
| `segment-index-lifecycle:openAndCheckAndRepairConsistency` | `278.221 ms/op` | `279.664 ms/op` | `+0.52%` | `neutral` |
| `segment-index-lifecycle:openAndCompact` | `302.197 ms/op` | `303.053 ms/op` | `+0.28%` | `neutral` |
| `segment-index-lifecycle:openExisting` | `277.958 ms/op` | `272.715 ms/op` | `-1.89%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed` | `497963.622 ops/s` | `509712.149 ops/s` | `+2.36%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed:getWorkload` | `492597.806 ops/s` | `504405.438 ops/s` | `+2.40%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed:putWorkload` | `5365.816 ops/s` | `5306.711 ops/s` | `-1.10%` | `neutral` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed` | `299423.976 ops/s` | `270665.625 ops/s` | `-9.60%` | `worse` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:getWorkload` | `264824.238 ops/s` | `269203.084 ops/s` | `+1.65%` | `neutral` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:putWorkload` | `34599.737 ops/s` | `1462.541 ops/s` | `-95.77%` | `worse` |
| `segment-index-persisted-mutation:deleteAsyncJoin` | `2490.226 ops/s` | `2486.900 ops/s` | `-0.13%` | `neutral` |
| `segment-index-persisted-mutation:deleteSync` | `2742.458 ops/s` | `2779.176 ops/s` | `+1.34%` | `neutral` |
| `segment-index-persisted-mutation:putAsyncJoin` | `2502.605 ops/s` | `2459.884 ops/s` | `-1.71%` | `neutral` |
| `segment-index-persisted-mutation:putSync` | `2667.355 ops/s` | `2658.867 ops/s` | `-0.32%` | `neutral` |
| `single-chunk-entry-write-compact:writeEntrySteadyState` | `8103090.528 ops/s` | `8210340.765 ops/s` | `+1.32%` | `neutral` |
| `single-chunk-entry-write-large:writeEntrySteadyState` | `7759428.817 ops/s` | `7828610.629 ops/s` | `+0.89%` | `neutral` |
| `sorted-data-diff-key-read-compact:readNextKey` | `8601164.853 ops/s` | `8630752.102 ops/s` | `+0.34%` | `neutral` |
| `sorted-data-diff-key-read-large:readNextKey` | `6660473.562 ops/s` | `6706352.358 ops/s` | `+0.69%` | `neutral` |
