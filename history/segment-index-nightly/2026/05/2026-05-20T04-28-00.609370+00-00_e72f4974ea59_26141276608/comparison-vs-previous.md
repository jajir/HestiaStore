# Benchmark Comparison

- Candidate labels rerun 3x total and median-merged because the initial regression exceeded threshold with high relative score error: `segment-index-get-multisegment-cold`


- Profile: `segment-index-nightly`
- Baseline SHA: `e72f4974ea597aa48d2725b131d13e39bcfa6872`
- Candidate SHA: `e72f4974ea597aa48d2725b131d13e39bcfa6872`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `segment-index-get-multisegment-cold:getHitAsyncJoin` | `40.045 ops/s` | `51.649 ops/s` | `+28.98%` | `better` |
| `segment-index-get-multisegment-cold:getHitSync` | `53.721 ops/s` | `45.401 ops/s` | `-15.49%` | `worse` |
| `segment-index-get-multisegment-cold:getMissAsyncJoin` | `175354.795 ops/s` | `184287.486 ops/s` | `+5.09%` | `better` |
| `segment-index-get-multisegment-cold:getMissSync` | `4056260.214 ops/s` | `3425006.672 ops/s` | `-15.56%` | `worse` |
| `segment-index-get-multisegment-hot:getHitAsyncJoin` | `97.900 ops/s` | `104.043 ops/s` | `+6.27%` | `better` |
| `segment-index-get-multisegment-hot:getHitSync` | `98.795 ops/s` | `96.648 ops/s` | `-2.17%` | `neutral` |
| `segment-index-get-multisegment-hot:getMissAsyncJoin` | `174196.761 ops/s` | `187838.719 ops/s` | `+7.83%` | `better` |
| `segment-index-get-multisegment-hot:getMissSync` | `3975175.298 ops/s` | `3620977.616 ops/s` | `-8.91%` | `worse` |
| `segment-index-get-overlay:getHitAsyncJoin` | `163740.344 ops/s` | `179651.815 ops/s` | `+9.72%` | `better` |
| `segment-index-get-overlay:getHitSync` | `4015791.521 ops/s` | `3767983.389 ops/s` | `-6.17%` | `warning` |
| `segment-index-get-overlay:getMissAsyncJoin` | `162453.526 ops/s` | `179155.860 ops/s` | `+10.28%` | `better` |
| `segment-index-get-overlay:getMissSync` | `3618481.090 ops/s` | `3696270.936 ops/s` | `+2.15%` | `neutral` |
| `segment-index-get-persisted:getHitAsyncJoin` | `63393.618 ops/s` | `65158.102 ops/s` | `+2.78%` | `neutral` |
| `segment-index-get-persisted:getHitSync` | `115094.311 ops/s` | `113094.388 ops/s` | `-1.74%` | `neutral` |
| `segment-index-get-persisted:getMissAsyncJoin` | `169599.404 ops/s` | `181603.634 ops/s` | `+7.08%` | `better` |
| `segment-index-get-persisted:getMissSync` | `3771913.949 ops/s` | `3267435.525 ops/s` | `-13.37%` | `worse` |
| `segment-index-hot-partition-put:putHotPartition` | `3049875.582 ops/s` | `2664618.750 ops/s` | `-12.63%` | `worse` |
| `segment-index-hot-partition-put:putThenGetHotPartition` | `1718393.530 ops/s` | `1289564.624 ops/s` | `-24.96%` | `worse` |
| `segment-index-lifecycle:openAndCheckAndRepairConsistency` | `243.377 ms/op` | `279.386 ms/op` | `+14.80%` | `better` |
| `segment-index-lifecycle:openAndCompact` | `267.450 ms/op` | `302.347 ms/op` | `+13.05%` | `better` |
| `segment-index-lifecycle:openExisting` | `242.371 ms/op` | `274.244 ms/op` | `+13.15%` | `better` |
| `segment-index-mixed-drain:partitionedIngestMixed` | `490372.319 ops/s` | `515573.479 ops/s` | `+5.14%` | `better` |
| `segment-index-mixed-drain:partitionedIngestMixed:getWorkload` | `485052.662 ops/s` | `510241.923 ops/s` | `+5.19%` | `better` |
| `segment-index-mixed-drain:partitionedIngestMixed:putWorkload` | `5319.658 ops/s` | `5331.556 ops/s` | `+0.22%` | `neutral` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed` | `267036.044 ops/s` | `302149.623 ops/s` | `+13.15%` | `better` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:getWorkload` | `265596.836 ops/s` | `266852.855 ops/s` | `+0.47%` | `neutral` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:putWorkload` | `1439.209 ops/s` | `35296.767 ops/s` | `+2352.51%` | `better` |
| `segment-index-persisted-mutation:deleteAsyncJoin` | `2013.606 ops/s` | `2436.703 ops/s` | `+21.01%` | `better` |
| `segment-index-persisted-mutation:deleteSync` | `2224.265 ops/s` | `2749.420 ops/s` | `+23.61%` | `better` |
| `segment-index-persisted-mutation:putAsyncJoin` | `1949.983 ops/s` | `2410.617 ops/s` | `+23.62%` | `better` |
| `segment-index-persisted-mutation:putSync` | `2124.962 ops/s` | `2594.774 ops/s` | `+22.11%` | `better` |
| `single-chunk-entry-write-compact:writeEntrySteadyState` | `8534708.814 ops/s` | `8261790.654 ops/s` | `-3.20%` | `warning` |
| `single-chunk-entry-write-large:writeEntrySteadyState` | `7864699.124 ops/s` | `7550676.595 ops/s` | `-3.99%` | `warning` |
| `sorted-data-diff-key-read-compact:readNextKey` | `8692263.916 ops/s` | `7802745.668 ops/s` | `-10.23%` | `worse` |
| `sorted-data-diff-key-read-large:readNextKey` | `6906600.987 ops/s` | `6752247.114 ops/s` | `-2.23%` | `neutral` |
