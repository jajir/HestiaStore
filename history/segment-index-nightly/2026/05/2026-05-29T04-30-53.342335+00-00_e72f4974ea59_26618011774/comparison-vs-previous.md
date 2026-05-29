# Benchmark Comparison

- Candidate labels rerun 3x total and median-merged because the initial regression exceeded threshold with high relative score error: `segment-index-get-multisegment-cold,segment-index-get-multisegment-hot`


- Profile: `segment-index-nightly`
- Baseline SHA: `e72f4974ea597aa48d2725b131d13e39bcfa6872`
- Candidate SHA: `e72f4974ea597aa48d2725b131d13e39bcfa6872`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `segment-index-get-multisegment-cold:getHitAsyncJoin` | `40.953 ops/s` | `42.283 ops/s` | `+3.25%` | `better` |
| `segment-index-get-multisegment-cold:getHitSync` | `50.211 ops/s` | `40.163 ops/s` | `-20.01%` | `worse` |
| `segment-index-get-multisegment-cold:getMissAsyncJoin` | `171590.656 ops/s` | `184060.478 ops/s` | `+7.27%` | `better` |
| `segment-index-get-multisegment-cold:getMissSync` | `3447503.365 ops/s` | `3706701.546 ops/s` | `+7.52%` | `better` |
| `segment-index-get-multisegment-hot:getHitAsyncJoin` | `87.914 ops/s` | `95.912 ops/s` | `+9.10%` | `better` |
| `segment-index-get-multisegment-hot:getHitSync` | `82.724 ops/s` | `97.316 ops/s` | `+17.64%` | `better` |
| `segment-index-get-multisegment-hot:getMissAsyncJoin` | `172139.333 ops/s` | `185457.018 ops/s` | `+7.74%` | `better` |
| `segment-index-get-multisegment-hot:getMissSync` | `3619217.386 ops/s` | `3640738.441 ops/s` | `+0.59%` | `neutral` |
| `segment-index-get-overlay:getHitAsyncJoin` | `161038.058 ops/s` | `178785.201 ops/s` | `+11.02%` | `better` |
| `segment-index-get-overlay:getHitSync` | `4450568.869 ops/s` | `3395410.857 ops/s` | `-23.71%` | `worse` |
| `segment-index-get-overlay:getMissAsyncJoin` | `157505.892 ops/s` | `184420.488 ops/s` | `+17.09%` | `better` |
| `segment-index-get-overlay:getMissSync` | `3707058.048 ops/s` | `3675723.306 ops/s` | `-0.85%` | `neutral` |
| `segment-index-get-persisted:getHitAsyncJoin` | `57723.066 ops/s` | `63448.516 ops/s` | `+9.92%` | `better` |
| `segment-index-get-persisted:getHitSync` | `113222.411 ops/s` | `115898.095 ops/s` | `+2.36%` | `neutral` |
| `segment-index-get-persisted:getMissAsyncJoin` | `155793.796 ops/s` | `184459.784 ops/s` | `+18.40%` | `better` |
| `segment-index-get-persisted:getMissSync` | `3730986.950 ops/s` | `3700446.532 ops/s` | `-0.82%` | `neutral` |
| `segment-index-hot-partition-put:putHotPartition` | `3226652.656 ops/s` | `2763152.740 ops/s` | `-14.36%` | `worse` |
| `segment-index-hot-partition-put:putThenGetHotPartition` | `1650811.044 ops/s` | `1573438.979 ops/s` | `-4.69%` | `warning` |
| `segment-index-lifecycle:openAndCheckAndRepairConsistency` | `248.024 ms/op` | `278.529 ms/op` | `+12.30%` | `better` |
| `segment-index-lifecycle:openAndCompact` | `269.491 ms/op` | `303.323 ms/op` | `+12.55%` | `better` |
| `segment-index-lifecycle:openExisting` | `244.899 ms/op` | `279.529 ms/op` | `+14.14%` | `better` |
| `segment-index-mixed-drain:partitionedIngestMixed` | `501501.849 ops/s` | `519262.216 ops/s` | `+3.54%` | `better` |
| `segment-index-mixed-drain:partitionedIngestMixed:getWorkload` | `496160.488 ops/s` | `513899.010 ops/s` | `+3.58%` | `better` |
| `segment-index-mixed-drain:partitionedIngestMixed:putWorkload` | `5341.362 ops/s` | `5363.206 ops/s` | `+0.41%` | `neutral` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed` | `264842.331 ops/s` | `266257.785 ops/s` | `+0.53%` | `neutral` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:getWorkload` | `263442.738 ops/s` | `264660.947 ops/s` | `+0.46%` | `neutral` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:putWorkload` | `1399.593 ops/s` | `1596.837 ops/s` | `+14.09%` | `better` |
| `segment-index-persisted-mutation:deleteAsyncJoin` | `1833.535 ops/s` | `2473.363 ops/s` | `+34.90%` | `better` |
| `segment-index-persisted-mutation:deleteSync` | `2163.103 ops/s` | `2749.228 ops/s` | `+27.10%` | `better` |
| `segment-index-persisted-mutation:putAsyncJoin` | `1942.643 ops/s` | `2513.363 ops/s` | `+29.38%` | `better` |
| `segment-index-persisted-mutation:putSync` | `2115.349 ops/s` | `2718.813 ops/s` | `+28.53%` | `better` |
| `single-chunk-entry-write-compact:writeEntrySteadyState` | `8482798.976 ops/s` | `8294022.299 ops/s` | `-2.23%` | `neutral` |
| `single-chunk-entry-write-large:writeEntrySteadyState` | `7960389.269 ops/s` | `7542780.406 ops/s` | `-5.25%` | `warning` |
| `sorted-data-diff-key-read-compact:readNextKey` | `8446217.589 ops/s` | `8570799.205 ops/s` | `+1.47%` | `neutral` |
| `sorted-data-diff-key-read-large:readNextKey` | `7492838.351 ops/s` | `6726920.913 ops/s` | `-10.22%` | `worse` |
