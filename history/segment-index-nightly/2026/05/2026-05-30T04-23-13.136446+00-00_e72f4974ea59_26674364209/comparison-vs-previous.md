# Benchmark Comparison

- Candidate labels rerun 3x total and median-merged because the initial regression exceeded threshold with high relative score error: `segment-index-get-multisegment-hot`


- Profile: `segment-index-nightly`
- Baseline SHA: `e72f4974ea597aa48d2725b131d13e39bcfa6872`
- Candidate SHA: `e72f4974ea597aa48d2725b131d13e39bcfa6872`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `segment-index-get-multisegment-cold:getHitAsyncJoin` | `42.283 ops/s` | `41.430 ops/s` | `-2.02%` | `neutral` |
| `segment-index-get-multisegment-cold:getHitSync` | `40.163 ops/s` | `38.603 ops/s` | `-3.88%` | `warning` |
| `segment-index-get-multisegment-cold:getMissAsyncJoin` | `184060.478 ops/s` | `181964.786 ops/s` | `-1.14%` | `neutral` |
| `segment-index-get-multisegment-cold:getMissSync` | `3706701.546 ops/s` | `3733206.985 ops/s` | `+0.72%` | `neutral` |
| `segment-index-get-multisegment-hot:getHitAsyncJoin` | `95.912 ops/s` | `96.101 ops/s` | `+0.20%` | `neutral` |
| `segment-index-get-multisegment-hot:getHitSync` | `97.316 ops/s` | `86.858 ops/s` | `-10.75%` | `worse` |
| `segment-index-get-multisegment-hot:getMissAsyncJoin` | `185457.018 ops/s` | `184106.643 ops/s` | `-0.73%` | `neutral` |
| `segment-index-get-multisegment-hot:getMissSync` | `3640738.441 ops/s` | `3903430.116 ops/s` | `+7.22%` | `better` |
| `segment-index-get-overlay:getHitAsyncJoin` | `178785.201 ops/s` | `174625.870 ops/s` | `-2.33%` | `neutral` |
| `segment-index-get-overlay:getHitSync` | `3395410.857 ops/s` | `4291872.083 ops/s` | `+26.40%` | `better` |
| `segment-index-get-overlay:getMissAsyncJoin` | `184420.488 ops/s` | `184954.936 ops/s` | `+0.29%` | `neutral` |
| `segment-index-get-overlay:getMissSync` | `3675723.306 ops/s` | `3526940.837 ops/s` | `-4.05%` | `warning` |
| `segment-index-get-persisted:getHitAsyncJoin` | `63448.516 ops/s` | `64430.058 ops/s` | `+1.55%` | `neutral` |
| `segment-index-get-persisted:getHitSync` | `115898.095 ops/s` | `111002.009 ops/s` | `-4.22%` | `warning` |
| `segment-index-get-persisted:getMissAsyncJoin` | `184459.784 ops/s` | `182890.107 ops/s` | `-0.85%` | `neutral` |
| `segment-index-get-persisted:getMissSync` | `3700446.532 ops/s` | `3984677.846 ops/s` | `+7.68%` | `better` |
| `segment-index-hot-partition-put:putHotPartition` | `2763152.740 ops/s` | `2835355.140 ops/s` | `+2.61%` | `neutral` |
| `segment-index-hot-partition-put:putThenGetHotPartition` | `1573438.979 ops/s` | `1616612.314 ops/s` | `+2.74%` | `neutral` |
| `segment-index-lifecycle:openAndCheckAndRepairConsistency` | `278.529 ms/op` | `280.252 ms/op` | `+0.62%` | `neutral` |
| `segment-index-lifecycle:openAndCompact` | `303.323 ms/op` | `305.329 ms/op` | `+0.66%` | `neutral` |
| `segment-index-lifecycle:openExisting` | `279.529 ms/op` | `278.202 ms/op` | `-0.47%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed` | `519262.216 ops/s` | `515632.314 ops/s` | `-0.70%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed:getWorkload` | `513899.010 ops/s` | `510207.704 ops/s` | `-0.72%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed:putWorkload` | `5363.206 ops/s` | `5424.610 ops/s` | `+1.14%` | `neutral` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed` | `266257.785 ops/s` | `282822.267 ops/s` | `+6.22%` | `better` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:getWorkload` | `264660.947 ops/s` | `267123.804 ops/s` | `+0.93%` | `neutral` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:putWorkload` | `1596.837 ops/s` | `15698.463 ops/s` | `+883.10%` | `better` |
| `segment-index-persisted-mutation:deleteAsyncJoin` | `2473.363 ops/s` | `2340.826 ops/s` | `-5.36%` | `warning` |
| `segment-index-persisted-mutation:deleteSync` | `2749.228 ops/s` | `2632.791 ops/s` | `-4.24%` | `warning` |
| `segment-index-persisted-mutation:putAsyncJoin` | `2513.363 ops/s` | `2363.886 ops/s` | `-5.95%` | `warning` |
| `segment-index-persisted-mutation:putSync` | `2718.813 ops/s` | `2618.687 ops/s` | `-3.68%` | `warning` |
| `single-chunk-entry-write-compact:writeEntrySteadyState` | `8294022.299 ops/s` | `8213557.986 ops/s` | `-0.97%` | `neutral` |
| `single-chunk-entry-write-large:writeEntrySteadyState` | `7542780.406 ops/s` | `7739893.039 ops/s` | `+2.61%` | `neutral` |
| `sorted-data-diff-key-read-compact:readNextKey` | `8570799.205 ops/s` | `8577483.454 ops/s` | `+0.08%` | `neutral` |
| `sorted-data-diff-key-read-large:readNextKey` | `6726920.913 ops/s` | `6762004.982 ops/s` | `+0.52%` | `neutral` |
