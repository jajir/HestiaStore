# Benchmark Comparison

- Candidate labels rerun 3x total and median-merged because the initial regression exceeded threshold with high relative score error: `segment-index-get-multisegment-cold,segment-index-get-multisegment-hot`


- Profile: `segment-index-nightly`
- Baseline SHA: `e72f4974ea597aa48d2725b131d13e39bcfa6872`
- Candidate SHA: `e72f4974ea597aa48d2725b131d13e39bcfa6872`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `segment-index-get-multisegment-cold:getHitAsyncJoin` | `39.748 ops/s` | `40.953 ops/s` | `+3.03%` | `better` |
| `segment-index-get-multisegment-cold:getHitSync` | `41.604 ops/s` | `50.211 ops/s` | `+20.69%` | `better` |
| `segment-index-get-multisegment-cold:getMissAsyncJoin` | `240081.241 ops/s` | `171590.656 ops/s` | `-28.53%` | `worse` |
| `segment-index-get-multisegment-cold:getMissSync` | `2583482.488 ops/s` | `3447503.365 ops/s` | `+33.44%` | `better` |
| `segment-index-get-multisegment-hot:getHitAsyncJoin` | `104.302 ops/s` | `87.914 ops/s` | `-15.71%` | `worse` |
| `segment-index-get-multisegment-hot:getHitSync` | `102.118 ops/s` | `82.724 ops/s` | `-18.99%` | `worse` |
| `segment-index-get-multisegment-hot:getMissAsyncJoin` | `240931.788 ops/s` | `172139.333 ops/s` | `-28.55%` | `worse` |
| `segment-index-get-multisegment-hot:getMissSync` | `2464056.760 ops/s` | `3619217.386 ops/s` | `+46.88%` | `better` |
| `segment-index-get-overlay:getHitAsyncJoin` | `232664.479 ops/s` | `161038.058 ops/s` | `-30.79%` | `worse` |
| `segment-index-get-overlay:getHitSync` | `2512338.518 ops/s` | `4450568.869 ops/s` | `+77.15%` | `better` |
| `segment-index-get-overlay:getMissAsyncJoin` | `235653.807 ops/s` | `157505.892 ops/s` | `-33.16%` | `worse` |
| `segment-index-get-overlay:getMissSync` | `2551024.102 ops/s` | `3707058.048 ops/s` | `+45.32%` | `better` |
| `segment-index-get-persisted:getHitAsyncJoin` | `78010.424 ops/s` | `57723.066 ops/s` | `-26.01%` | `worse` |
| `segment-index-get-persisted:getHitSync` | `112657.392 ops/s` | `113222.411 ops/s` | `+0.50%` | `neutral` |
| `segment-index-get-persisted:getMissAsyncJoin` | `228443.905 ops/s` | `155793.796 ops/s` | `-31.80%` | `worse` |
| `segment-index-get-persisted:getMissSync` | `2536185.395 ops/s` | `3730986.950 ops/s` | `+47.11%` | `better` |
| `segment-index-hot-partition-put:putHotPartition` | `1909693.932 ops/s` | `3226652.656 ops/s` | `+68.96%` | `better` |
| `segment-index-hot-partition-put:putThenGetHotPartition` | `1009142.163 ops/s` | `1650811.044 ops/s` | `+63.59%` | `better` |
| `segment-index-lifecycle:openAndCheckAndRepairConsistency` | `133.979 ms/op` | `248.024 ms/op` | `+85.12%` | `better` |
| `segment-index-lifecycle:openAndCompact` | `155.952 ms/op` | `269.491 ms/op` | `+72.80%` | `better` |
| `segment-index-lifecycle:openExisting` | `131.863 ms/op` | `244.899 ms/op` | `+85.72%` | `better` |
| `segment-index-mixed-drain:partitionedIngestMixed` | `459349.424 ops/s` | `501501.849 ops/s` | `+9.18%` | `better` |
| `segment-index-mixed-drain:partitionedIngestMixed:getWorkload` | `453980.614 ops/s` | `496160.488 ops/s` | `+9.29%` | `better` |
| `segment-index-mixed-drain:partitionedIngestMixed:putWorkload` | `5368.810 ops/s` | `5341.362 ops/s` | `-0.51%` | `neutral` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed` | `243565.342 ops/s` | `264842.331 ops/s` | `+8.74%` | `better` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:getWorkload` | `242089.367 ops/s` | `263442.738 ops/s` | `+8.82%` | `better` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:putWorkload` | `1475.974 ops/s` | `1399.593 ops/s` | `-5.17%` | `warning` |
| `segment-index-persisted-mutation:deleteAsyncJoin` | `2485.156 ops/s` | `1833.535 ops/s` | `-26.22%` | `worse` |
| `segment-index-persisted-mutation:deleteSync` | `2754.755 ops/s` | `2163.103 ops/s` | `-21.48%` | `worse` |
| `segment-index-persisted-mutation:putAsyncJoin` | `2505.136 ops/s` | `1942.643 ops/s` | `-22.45%` | `worse` |
| `segment-index-persisted-mutation:putSync` | `2690.224 ops/s` | `2115.349 ops/s` | `-21.37%` | `worse` |
| `single-chunk-entry-write-compact:writeEntrySteadyState` | `8893172.402 ops/s` | `8482798.976 ops/s` | `-4.61%` | `warning` |
| `single-chunk-entry-write-large:writeEntrySteadyState` | `8051805.162 ops/s` | `7960389.269 ops/s` | `-1.14%` | `neutral` |
| `sorted-data-diff-key-read-compact:readNextKey` | `8156597.829 ops/s` | `8446217.589 ops/s` | `+3.55%` | `better` |
| `sorted-data-diff-key-read-large:readNextKey` | `7029765.264 ops/s` | `7492838.351 ops/s` | `+6.59%` | `better` |
