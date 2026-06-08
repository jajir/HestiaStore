# Benchmark Comparison

- Profile: `segment-index-nightly`
- Baseline SHA: `e72f4974ea597aa48d2725b131d13e39bcfa6872`
- Candidate SHA: `e72f4974ea597aa48d2725b131d13e39bcfa6872`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `segment-index-get-multisegment-cold:getHitAsyncJoin` | `41.658 ops/s` | `51.129 ops/s` | `+22.73%` | `better` |
| `segment-index-get-multisegment-cold:getHitSync` | `36.593 ops/s` | `51.329 ops/s` | `+40.27%` | `better` |
| `segment-index-get-multisegment-cold:getMissAsyncJoin` | `184803.049 ops/s` | `183966.385 ops/s` | `-0.45%` | `neutral` |
| `segment-index-get-multisegment-cold:getMissSync` | `3566520.046 ops/s` | `3836626.677 ops/s` | `+7.57%` | `better` |
| `segment-index-get-multisegment-hot:getHitAsyncJoin` | `96.391 ops/s` | `103.448 ops/s` | `+7.32%` | `better` |
| `segment-index-get-multisegment-hot:getHitSync` | `97.812 ops/s` | `100.357 ops/s` | `+2.60%` | `neutral` |
| `segment-index-get-multisegment-hot:getMissAsyncJoin` | `183670.382 ops/s` | `185948.038 ops/s` | `+1.24%` | `neutral` |
| `segment-index-get-multisegment-hot:getMissSync` | `3820156.299 ops/s` | `3776749.121 ops/s` | `-1.14%` | `neutral` |
| `segment-index-get-overlay:getHitAsyncJoin` | `179163.070 ops/s` | `179081.049 ops/s` | `-0.05%` | `neutral` |
| `segment-index-get-overlay:getHitSync` | `3720827.553 ops/s` | `3491744.503 ops/s` | `-6.16%` | `warning` |
| `segment-index-get-overlay:getMissAsyncJoin` | `185213.757 ops/s` | `187288.686 ops/s` | `+1.12%` | `neutral` |
| `segment-index-get-overlay:getMissSync` | `3881562.090 ops/s` | `3765874.173 ops/s` | `-2.98%` | `neutral` |
| `segment-index-get-persisted:getHitAsyncJoin` | `68086.944 ops/s` | `63153.664 ops/s` | `-7.25%` | `worse` |
| `segment-index-get-persisted:getHitSync` | `115640.220 ops/s` | `114357.581 ops/s` | `-1.11%` | `neutral` |
| `segment-index-get-persisted:getMissAsyncJoin` | `183973.882 ops/s` | `179642.320 ops/s` | `-2.35%` | `neutral` |
| `segment-index-get-persisted:getMissSync` | `3493071.711 ops/s` | `3493042.803 ops/s` | `-0.00%` | `neutral` |
| `segment-index-hot-partition-put:putHotPartition` | `2769865.181 ops/s` | `2683150.631 ops/s` | `-3.13%` | `warning` |
| `segment-index-hot-partition-put:putThenGetHotPartition` | `1491829.651 ops/s` | `1448324.938 ops/s` | `-2.92%` | `neutral` |
| `segment-index-lifecycle:openAndCheckAndRepairConsistency` | `279.664 ms/op` | `278.817 ms/op` | `-0.30%` | `neutral` |
| `segment-index-lifecycle:openAndCompact` | `303.053 ms/op` | `300.150 ms/op` | `-0.96%` | `neutral` |
| `segment-index-lifecycle:openExisting` | `272.715 ms/op` | `275.502 ms/op` | `+1.02%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed` | `509712.149 ops/s` | `516989.773 ops/s` | `+1.43%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed:getWorkload` | `504405.438 ops/s` | `511611.375 ops/s` | `+1.43%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed:putWorkload` | `5306.711 ops/s` | `5378.398 ops/s` | `+1.35%` | `neutral` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed` | `270665.625 ops/s` | `267962.011 ops/s` | `-1.00%` | `neutral` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:getWorkload` | `269203.084 ops/s` | `266483.074 ops/s` | `-1.01%` | `neutral` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:putWorkload` | `1462.541 ops/s` | `1478.937 ops/s` | `+1.12%` | `neutral` |
| `segment-index-persisted-mutation:deleteAsyncJoin` | `2486.900 ops/s` | `2514.484 ops/s` | `+1.11%` | `neutral` |
| `segment-index-persisted-mutation:deleteSync` | `2779.176 ops/s` | `2732.674 ops/s` | `-1.67%` | `neutral` |
| `segment-index-persisted-mutation:putAsyncJoin` | `2459.884 ops/s` | `2438.330 ops/s` | `-0.88%` | `neutral` |
| `segment-index-persisted-mutation:putSync` | `2658.867 ops/s` | `2706.670 ops/s` | `+1.80%` | `neutral` |
| `single-chunk-entry-write-compact:writeEntrySteadyState` | `8210340.765 ops/s` | `8265493.242 ops/s` | `+0.67%` | `neutral` |
| `single-chunk-entry-write-large:writeEntrySteadyState` | `7828610.629 ops/s` | `7738461.386 ops/s` | `-1.15%` | `neutral` |
| `sorted-data-diff-key-read-compact:readNextKey` | `8630752.102 ops/s` | `8614189.403 ops/s` | `-0.19%` | `neutral` |
| `sorted-data-diff-key-read-large:readNextKey` | `6706352.358 ops/s` | `6696807.836 ops/s` | `-0.14%` | `neutral` |
