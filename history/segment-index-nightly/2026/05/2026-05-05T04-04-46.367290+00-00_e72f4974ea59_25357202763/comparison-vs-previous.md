# Benchmark Comparison

- Candidate labels rerun 3x total and median-merged because the initial regression exceeded threshold with high relative score error: `segment-index-get-multisegment-hot`


- Profile: `segment-index-nightly`
- Baseline SHA: `e72f4974ea597aa48d2725b131d13e39bcfa6872`
- Candidate SHA: `e72f4974ea597aa48d2725b131d13e39bcfa6872`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `segment-index-get-multisegment-cold:getHitAsyncJoin` | `46.290 ops/s` | `44.062 ops/s` | `-4.81%` | `warning` |
| `segment-index-get-multisegment-cold:getHitSync` | `48.334 ops/s` | `45.831 ops/s` | `-5.18%` | `warning` |
| `segment-index-get-multisegment-cold:getMissAsyncJoin` | `171715.756 ops/s` | `170327.976 ops/s` | `-0.81%` | `neutral` |
| `segment-index-get-multisegment-cold:getMissSync` | `3831807.317 ops/s` | `3850340.982 ops/s` | `+0.48%` | `neutral` |
| `segment-index-get-multisegment-hot:getHitAsyncJoin` | `102.618 ops/s` | `89.806 ops/s` | `-12.49%` | `worse` |
| `segment-index-get-multisegment-hot:getHitSync` | `84.399 ops/s` | `86.156 ops/s` | `+2.08%` | `neutral` |
| `segment-index-get-multisegment-hot:getMissAsyncJoin` | `172312.608 ops/s` | `171564.189 ops/s` | `-0.43%` | `neutral` |
| `segment-index-get-multisegment-hot:getMissSync` | `3777903.414 ops/s` | `3784148.309 ops/s` | `+0.17%` | `neutral` |
| `segment-index-get-overlay:getHitAsyncJoin` | `162744.509 ops/s` | `163646.503 ops/s` | `+0.55%` | `neutral` |
| `segment-index-get-overlay:getHitSync` | `3699562.911 ops/s` | `3706394.083 ops/s` | `+0.18%` | `neutral` |
| `segment-index-get-overlay:getMissAsyncJoin` | `165205.508 ops/s` | `159855.039 ops/s` | `-3.24%` | `warning` |
| `segment-index-get-overlay:getMissSync` | `3844601.370 ops/s` | `3632446.071 ops/s` | `-5.52%` | `warning` |
| `segment-index-get-persisted:getHitAsyncJoin` | `65240.515 ops/s` | `64368.410 ops/s` | `-1.34%` | `neutral` |
| `segment-index-get-persisted:getHitSync` | `116764.210 ops/s` | `117745.165 ops/s` | `+0.84%` | `neutral` |
| `segment-index-get-persisted:getMissAsyncJoin` | `165328.115 ops/s` | `157048.904 ops/s` | `-5.01%` | `warning` |
| `segment-index-get-persisted:getMissSync` | `3573890.580 ops/s` | `3822988.202 ops/s` | `+6.97%` | `better` |
| `segment-index-hot-partition-put:putHotPartition` | `2959976.022 ops/s` | `3000255.403 ops/s` | `+1.36%` | `neutral` |
| `segment-index-hot-partition-put:putThenGetHotPartition` | `1727755.127 ops/s` | `1651658.402 ops/s` | `-4.40%` | `warning` |
| `segment-index-lifecycle:openAndCheckAndRepairConsistency` | `243.624 ms/op` | `246.944 ms/op` | `+1.36%` | `neutral` |
| `segment-index-lifecycle:openAndCompact` | `264.901 ms/op` | `270.730 ms/op` | `+2.20%` | `neutral` |
| `segment-index-lifecycle:openExisting` | `240.183 ms/op` | `245.156 ms/op` | `+2.07%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed` | `529038.216 ops/s` | `494126.209 ops/s` | `-6.60%` | `warning` |
| `segment-index-mixed-drain:partitionedIngestMixed:getWorkload` | `523731.134 ops/s` | `488810.992 ops/s` | `-6.67%` | `warning` |
| `segment-index-mixed-drain:partitionedIngestMixed:putWorkload` | `5307.081 ops/s` | `5315.217 ops/s` | `+0.15%` | `neutral` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed` | `266137.643 ops/s` | `277436.925 ops/s` | `+4.25%` | `better` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:getWorkload` | `264710.601 ops/s` | `275929.008 ops/s` | `+4.24%` | `better` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:putWorkload` | `1427.042 ops/s` | `1507.917 ops/s` | `+5.67%` | `better` |
| `segment-index-persisted-mutation:deleteAsyncJoin` | `1997.562 ops/s` | `2010.706 ops/s` | `+0.66%` | `neutral` |
| `segment-index-persisted-mutation:deleteSync` | `2221.430 ops/s` | `2229.967 ops/s` | `+0.38%` | `neutral` |
| `segment-index-persisted-mutation:putAsyncJoin` | `1979.378 ops/s` | `1991.932 ops/s` | `+0.63%` | `neutral` |
| `segment-index-persisted-mutation:putSync` | `2166.124 ops/s` | `2151.577 ops/s` | `-0.67%` | `neutral` |
| `single-chunk-entry-write-compact:writeEntrySteadyState` | `8480804.073 ops/s` | `8512493.219 ops/s` | `+0.37%` | `neutral` |
| `single-chunk-entry-write-large:writeEntrySteadyState` | `7571023.331 ops/s` | `7898115.728 ops/s` | `+4.32%` | `better` |
| `sorted-data-diff-key-read-compact:readNextKey` | `9459641.674 ops/s` | `9497451.988 ops/s` | `+0.40%` | `neutral` |
| `sorted-data-diff-key-read-large:readNextKey` | `7328077.334 ops/s` | `7454282.281 ops/s` | `+1.72%` | `neutral` |
