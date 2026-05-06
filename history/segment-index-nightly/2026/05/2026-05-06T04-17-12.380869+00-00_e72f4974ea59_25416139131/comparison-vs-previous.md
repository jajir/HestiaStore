# Benchmark Comparison

- Candidate labels rerun 3x total and median-merged because the initial regression exceeded threshold with high relative score error: `segment-index-get-multisegment-cold`


- Profile: `segment-index-nightly`
- Baseline SHA: `e72f4974ea597aa48d2725b131d13e39bcfa6872`
- Candidate SHA: `e72f4974ea597aa48d2725b131d13e39bcfa6872`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `segment-index-get-multisegment-cold:getHitAsyncJoin` | `44.062 ops/s` | `46.483 ops/s` | `+5.49%` | `better` |
| `segment-index-get-multisegment-cold:getHitSync` | `45.831 ops/s` | `38.453 ops/s` | `-16.10%` | `worse` |
| `segment-index-get-multisegment-cold:getMissAsyncJoin` | `170327.976 ops/s` | `172544.634 ops/s` | `+1.30%` | `neutral` |
| `segment-index-get-multisegment-cold:getMissSync` | `3850340.982 ops/s` | `3695242.139 ops/s` | `-4.03%` | `warning` |
| `segment-index-get-multisegment-hot:getHitAsyncJoin` | `89.806 ops/s` | `83.895 ops/s` | `-6.58%` | `warning` |
| `segment-index-get-multisegment-hot:getHitSync` | `86.156 ops/s` | `93.095 ops/s` | `+8.05%` | `better` |
| `segment-index-get-multisegment-hot:getMissAsyncJoin` | `171564.189 ops/s` | `174416.733 ops/s` | `+1.66%` | `neutral` |
| `segment-index-get-multisegment-hot:getMissSync` | `3784148.309 ops/s` | `3666893.527 ops/s` | `-3.10%` | `warning` |
| `segment-index-get-overlay:getHitAsyncJoin` | `163646.503 ops/s` | `159222.425 ops/s` | `-2.70%` | `neutral` |
| `segment-index-get-overlay:getHitSync` | `3706394.083 ops/s` | `3883242.354 ops/s` | `+4.77%` | `better` |
| `segment-index-get-overlay:getMissAsyncJoin` | `159855.039 ops/s` | `167409.608 ops/s` | `+4.73%` | `better` |
| `segment-index-get-overlay:getMissSync` | `3632446.071 ops/s` | `4193871.086 ops/s` | `+15.46%` | `better` |
| `segment-index-get-persisted:getHitAsyncJoin` | `64368.410 ops/s` | `64840.886 ops/s` | `+0.73%` | `neutral` |
| `segment-index-get-persisted:getHitSync` | `117745.165 ops/s` | `114507.581 ops/s` | `-2.75%` | `neutral` |
| `segment-index-get-persisted:getMissAsyncJoin` | `157048.904 ops/s` | `159639.799 ops/s` | `+1.65%` | `neutral` |
| `segment-index-get-persisted:getMissSync` | `3822988.202 ops/s` | `3787460.460 ops/s` | `-0.93%` | `neutral` |
| `segment-index-hot-partition-put:putHotPartition` | `3000255.403 ops/s` | `2860938.750 ops/s` | `-4.64%` | `warning` |
| `segment-index-hot-partition-put:putThenGetHotPartition` | `1651658.402 ops/s` | `1519966.557 ops/s` | `-7.97%` | `worse` |
| `segment-index-lifecycle:openAndCheckAndRepairConsistency` | `246.944 ms/op` | `241.705 ms/op` | `-2.12%` | `neutral` |
| `segment-index-lifecycle:openAndCompact` | `270.730 ms/op` | `263.681 ms/op` | `-2.60%` | `neutral` |
| `segment-index-lifecycle:openExisting` | `245.156 ms/op` | `240.048 ms/op` | `-2.08%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed` | `494126.209 ops/s` | `518220.031 ops/s` | `+4.88%` | `better` |
| `segment-index-mixed-drain:partitionedIngestMixed:getWorkload` | `488810.992 ops/s` | `512844.778 ops/s` | `+4.92%` | `better` |
| `segment-index-mixed-drain:partitionedIngestMixed:putWorkload` | `5315.217 ops/s` | `5375.253 ops/s` | `+1.13%` | `neutral` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed` | `277436.925 ops/s` | `263131.070 ops/s` | `-5.16%` | `warning` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:getWorkload` | `275929.008 ops/s` | `261631.100 ops/s` | `-5.18%` | `warning` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:putWorkload` | `1507.917 ops/s` | `1499.970 ops/s` | `-0.53%` | `neutral` |
| `segment-index-persisted-mutation:deleteAsyncJoin` | `2010.706 ops/s` | `2009.236 ops/s` | `-0.07%` | `neutral` |
| `segment-index-persisted-mutation:deleteSync` | `2229.967 ops/s` | `2242.993 ops/s` | `+0.58%` | `neutral` |
| `segment-index-persisted-mutation:putAsyncJoin` | `1991.932 ops/s` | `1993.787 ops/s` | `+0.09%` | `neutral` |
| `segment-index-persisted-mutation:putSync` | `2151.577 ops/s` | `2171.119 ops/s` | `+0.91%` | `neutral` |
| `single-chunk-entry-write-compact:writeEntrySteadyState` | `8512493.219 ops/s` | `8551272.678 ops/s` | `+0.46%` | `neutral` |
| `single-chunk-entry-write-large:writeEntrySteadyState` | `7898115.728 ops/s` | `7980889.145 ops/s` | `+1.05%` | `neutral` |
| `sorted-data-diff-key-read-compact:readNextKey` | `9497451.988 ops/s` | `8606323.182 ops/s` | `-9.38%` | `worse` |
| `sorted-data-diff-key-read-large:readNextKey` | `7454282.281 ops/s` | `7352703.231 ops/s` | `-1.36%` | `neutral` |
