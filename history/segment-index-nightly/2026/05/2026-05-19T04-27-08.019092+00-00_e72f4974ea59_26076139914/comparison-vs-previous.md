# Benchmark Comparison

- Profile: `segment-index-nightly`
- Baseline SHA: `e72f4974ea597aa48d2725b131d13e39bcfa6872`
- Candidate SHA: `e72f4974ea597aa48d2725b131d13e39bcfa6872`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `segment-index-get-multisegment-cold:getHitAsyncJoin` | `41.137 ops/s` | `40.045 ops/s` | `-2.65%` | `neutral` |
| `segment-index-get-multisegment-cold:getHitSync` | `36.556 ops/s` | `53.721 ops/s` | `+46.96%` | `better` |
| `segment-index-get-multisegment-cold:getMissAsyncJoin` | `171818.633 ops/s` | `175354.795 ops/s` | `+2.06%` | `neutral` |
| `segment-index-get-multisegment-cold:getMissSync` | `3797502.280 ops/s` | `4056260.214 ops/s` | `+6.81%` | `better` |
| `segment-index-get-multisegment-hot:getHitAsyncJoin` | `87.386 ops/s` | `97.900 ops/s` | `+12.03%` | `better` |
| `segment-index-get-multisegment-hot:getHitSync` | `88.092 ops/s` | `98.795 ops/s` | `+12.15%` | `better` |
| `segment-index-get-multisegment-hot:getMissAsyncJoin` | `171749.827 ops/s` | `174196.761 ops/s` | `+1.42%` | `neutral` |
| `segment-index-get-multisegment-hot:getMissSync` | `3771431.721 ops/s` | `3975175.298 ops/s` | `+5.40%` | `better` |
| `segment-index-get-overlay:getHitAsyncJoin` | `163330.613 ops/s` | `163740.344 ops/s` | `+0.25%` | `neutral` |
| `segment-index-get-overlay:getHitSync` | `3925493.140 ops/s` | `4015791.521 ops/s` | `+2.30%` | `neutral` |
| `segment-index-get-overlay:getMissAsyncJoin` | `163548.908 ops/s` | `162453.526 ops/s` | `-0.67%` | `neutral` |
| `segment-index-get-overlay:getMissSync` | `3930957.276 ops/s` | `3618481.090 ops/s` | `-7.95%` | `worse` |
| `segment-index-get-persisted:getHitAsyncJoin` | `60743.910 ops/s` | `63393.618 ops/s` | `+4.36%` | `better` |
| `segment-index-get-persisted:getHitSync` | `114836.156 ops/s` | `115094.311 ops/s` | `+0.22%` | `neutral` |
| `segment-index-get-persisted:getMissAsyncJoin` | `162471.411 ops/s` | `169599.404 ops/s` | `+4.39%` | `better` |
| `segment-index-get-persisted:getMissSync` | `4142267.752 ops/s` | `3771913.949 ops/s` | `-8.94%` | `worse` |
| `segment-index-hot-partition-put:putHotPartition` | `3060291.466 ops/s` | `3049875.582 ops/s` | `-0.34%` | `neutral` |
| `segment-index-hot-partition-put:putThenGetHotPartition` | `1424431.791 ops/s` | `1718393.530 ops/s` | `+20.64%` | `better` |
| `segment-index-lifecycle:openAndCheckAndRepairConsistency` | `249.998 ms/op` | `243.377 ms/op` | `-2.65%` | `neutral` |
| `segment-index-lifecycle:openAndCompact` | `266.123 ms/op` | `267.450 ms/op` | `+0.50%` | `neutral` |
| `segment-index-lifecycle:openExisting` | `243.906 ms/op` | `242.371 ms/op` | `-0.63%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed` | `500843.266 ops/s` | `490372.319 ops/s` | `-2.09%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed:getWorkload` | `495492.157 ops/s` | `485052.662 ops/s` | `-2.11%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed:putWorkload` | `5351.109 ops/s` | `5319.658 ops/s` | `-0.59%` | `neutral` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed` | `256151.297 ops/s` | `267036.044 ops/s` | `+4.25%` | `better` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:getWorkload` | `254614.152 ops/s` | `265596.836 ops/s` | `+4.31%` | `better` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:putWorkload` | `1537.146 ops/s` | `1439.209 ops/s` | `-6.37%` | `warning` |
| `segment-index-persisted-mutation:deleteAsyncJoin` | `2000.882 ops/s` | `2013.606 ops/s` | `+0.64%` | `neutral` |
| `segment-index-persisted-mutation:deleteSync` | `2230.224 ops/s` | `2224.265 ops/s` | `-0.27%` | `neutral` |
| `segment-index-persisted-mutation:putAsyncJoin` | `1969.908 ops/s` | `1949.983 ops/s` | `-1.01%` | `neutral` |
| `segment-index-persisted-mutation:putSync` | `2169.378 ops/s` | `2124.962 ops/s` | `-2.05%` | `neutral` |
| `single-chunk-entry-write-compact:writeEntrySteadyState` | `8480398.023 ops/s` | `8534708.814 ops/s` | `+0.64%` | `neutral` |
| `single-chunk-entry-write-large:writeEntrySteadyState` | `7850816.189 ops/s` | `7864699.124 ops/s` | `+0.18%` | `neutral` |
| `sorted-data-diff-key-read-compact:readNextKey` | `9140482.694 ops/s` | `8692263.916 ops/s` | `-4.90%` | `warning` |
| `sorted-data-diff-key-read-large:readNextKey` | `7377788.122 ops/s` | `6906600.987 ops/s` | `-6.39%` | `warning` |
