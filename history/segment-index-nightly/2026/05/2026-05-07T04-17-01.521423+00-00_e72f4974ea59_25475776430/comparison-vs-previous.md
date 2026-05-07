# Benchmark Comparison

- Profile: `segment-index-nightly`
- Baseline SHA: `e72f4974ea597aa48d2725b131d13e39bcfa6872`
- Candidate SHA: `e72f4974ea597aa48d2725b131d13e39bcfa6872`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `segment-index-get-multisegment-cold:getHitAsyncJoin` | `46.483 ops/s` | `45.037 ops/s` | `-3.11%` | `warning` |
| `segment-index-get-multisegment-cold:getHitSync` | `38.453 ops/s` | `43.521 ops/s` | `+13.18%` | `better` |
| `segment-index-get-multisegment-cold:getMissAsyncJoin` | `172544.634 ops/s` | `170761.681 ops/s` | `-1.03%` | `neutral` |
| `segment-index-get-multisegment-cold:getMissSync` | `3695242.139 ops/s` | `3689382.510 ops/s` | `-0.16%` | `neutral` |
| `segment-index-get-multisegment-hot:getHitAsyncJoin` | `83.895 ops/s` | `82.315 ops/s` | `-1.88%` | `neutral` |
| `segment-index-get-multisegment-hot:getHitSync` | `93.095 ops/s` | `88.310 ops/s` | `-5.14%` | `warning` |
| `segment-index-get-multisegment-hot:getMissAsyncJoin` | `174416.733 ops/s` | `168972.374 ops/s` | `-3.12%` | `warning` |
| `segment-index-get-multisegment-hot:getMissSync` | `3666893.527 ops/s` | `3729614.593 ops/s` | `+1.71%` | `neutral` |
| `segment-index-get-overlay:getHitAsyncJoin` | `159222.425 ops/s` | `160192.186 ops/s` | `+0.61%` | `neutral` |
| `segment-index-get-overlay:getHitSync` | `3883242.354 ops/s` | `3822420.043 ops/s` | `-1.57%` | `neutral` |
| `segment-index-get-overlay:getMissAsyncJoin` | `167409.608 ops/s` | `162963.962 ops/s` | `-2.66%` | `neutral` |
| `segment-index-get-overlay:getMissSync` | `4193871.086 ops/s` | `3925160.617 ops/s` | `-6.41%` | `warning` |
| `segment-index-get-persisted:getHitAsyncJoin` | `64840.886 ops/s` | `60868.196 ops/s` | `-6.13%` | `warning` |
| `segment-index-get-persisted:getHitSync` | `114507.581 ops/s` | `114944.213 ops/s` | `+0.38%` | `neutral` |
| `segment-index-get-persisted:getMissAsyncJoin` | `159639.799 ops/s` | `164812.017 ops/s` | `+3.24%` | `better` |
| `segment-index-get-persisted:getMissSync` | `3787460.460 ops/s` | `3762276.370 ops/s` | `-0.66%` | `neutral` |
| `segment-index-hot-partition-put:putHotPartition` | `2860938.750 ops/s` | `3019337.790 ops/s` | `+5.54%` | `better` |
| `segment-index-hot-partition-put:putThenGetHotPartition` | `1519966.557 ops/s` | `1688793.418 ops/s` | `+11.11%` | `better` |
| `segment-index-lifecycle:openAndCheckAndRepairConsistency` | `241.705 ms/op` | `246.450 ms/op` | `+1.96%` | `neutral` |
| `segment-index-lifecycle:openAndCompact` | `263.681 ms/op` | `268.094 ms/op` | `+1.67%` | `neutral` |
| `segment-index-lifecycle:openExisting` | `240.048 ms/op` | `244.533 ms/op` | `+1.87%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed` | `518220.031 ops/s` | `495566.823 ops/s` | `-4.37%` | `warning` |
| `segment-index-mixed-drain:partitionedIngestMixed:getWorkload` | `512844.778 ops/s` | `490199.499 ops/s` | `-4.42%` | `warning` |
| `segment-index-mixed-drain:partitionedIngestMixed:putWorkload` | `5375.253 ops/s` | `5367.325 ops/s` | `-0.15%` | `neutral` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed` | `263131.070 ops/s` | `306037.392 ops/s` | `+16.31%` | `better` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:getWorkload` | `261631.100 ops/s` | `259977.126 ops/s` | `-0.63%` | `neutral` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:putWorkload` | `1499.970 ops/s` | `46060.267 ops/s` | `+2970.75%` | `better` |
| `segment-index-persisted-mutation:deleteAsyncJoin` | `2009.236 ops/s` | `2028.569 ops/s` | `+0.96%` | `neutral` |
| `segment-index-persisted-mutation:deleteSync` | `2242.993 ops/s` | `2281.802 ops/s` | `+1.73%` | `neutral` |
| `segment-index-persisted-mutation:putAsyncJoin` | `1993.787 ops/s` | `2010.657 ops/s` | `+0.85%` | `neutral` |
| `segment-index-persisted-mutation:putSync` | `2171.119 ops/s` | `2239.315 ops/s` | `+3.14%` | `better` |
| `single-chunk-entry-write-compact:writeEntrySteadyState` | `8551272.678 ops/s` | `8501399.426 ops/s` | `-0.58%` | `neutral` |
| `single-chunk-entry-write-large:writeEntrySteadyState` | `7980889.145 ops/s` | `7952282.319 ops/s` | `-0.36%` | `neutral` |
| `sorted-data-diff-key-read-compact:readNextKey` | `8606323.182 ops/s` | `8633835.358 ops/s` | `+0.32%` | `neutral` |
| `sorted-data-diff-key-read-large:readNextKey` | `7352703.231 ops/s` | `7432550.234 ops/s` | `+1.09%` | `neutral` |
