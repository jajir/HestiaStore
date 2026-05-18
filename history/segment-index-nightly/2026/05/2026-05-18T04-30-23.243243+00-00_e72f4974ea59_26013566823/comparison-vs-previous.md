# Benchmark Comparison

- Candidate labels rerun 3x total and median-merged because the initial regression exceeded threshold with high relative score error: `segment-index-get-multisegment-cold,segment-index-get-multisegment-hot`


- Profile: `segment-index-nightly`
- Baseline SHA: `e72f4974ea597aa48d2725b131d13e39bcfa6872`
- Candidate SHA: `e72f4974ea597aa48d2725b131d13e39bcfa6872`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `segment-index-get-multisegment-cold:getHitAsyncJoin` | `46.730 ops/s` | `41.137 ops/s` | `-11.97%` | `worse` |
| `segment-index-get-multisegment-cold:getHitSync` | `47.235 ops/s` | `36.556 ops/s` | `-22.61%` | `worse` |
| `segment-index-get-multisegment-cold:getMissAsyncJoin` | `173636.729 ops/s` | `171818.633 ops/s` | `-1.05%` | `neutral` |
| `segment-index-get-multisegment-cold:getMissSync` | `3515044.466 ops/s` | `3797502.280 ops/s` | `+8.04%` | `better` |
| `segment-index-get-multisegment-hot:getHitAsyncJoin` | `123.318 ops/s` | `87.386 ops/s` | `-29.14%` | `worse` |
| `segment-index-get-multisegment-hot:getHitSync` | `99.273 ops/s` | `88.092 ops/s` | `-11.26%` | `worse` |
| `segment-index-get-multisegment-hot:getMissAsyncJoin` | `173213.678 ops/s` | `171749.827 ops/s` | `-0.85%` | `neutral` |
| `segment-index-get-multisegment-hot:getMissSync` | `3744586.415 ops/s` | `3771431.721 ops/s` | `+0.72%` | `neutral` |
| `segment-index-get-overlay:getHitAsyncJoin` | `152997.241 ops/s` | `163330.613 ops/s` | `+6.75%` | `better` |
| `segment-index-get-overlay:getHitSync` | `4281968.552 ops/s` | `3925493.140 ops/s` | `-8.33%` | `worse` |
| `segment-index-get-overlay:getMissAsyncJoin` | `160687.259 ops/s` | `163548.908 ops/s` | `+1.78%` | `neutral` |
| `segment-index-get-overlay:getMissSync` | `3752476.737 ops/s` | `3930957.276 ops/s` | `+4.76%` | `better` |
| `segment-index-get-persisted:getHitAsyncJoin` | `62421.308 ops/s` | `60743.910 ops/s` | `-2.69%` | `neutral` |
| `segment-index-get-persisted:getHitSync` | `115241.311 ops/s` | `114836.156 ops/s` | `-0.35%` | `neutral` |
| `segment-index-get-persisted:getMissAsyncJoin` | `157477.056 ops/s` | `162471.411 ops/s` | `+3.17%` | `better` |
| `segment-index-get-persisted:getMissSync` | `3663306.335 ops/s` | `4142267.752 ops/s` | `+13.07%` | `better` |
| `segment-index-hot-partition-put:putHotPartition` | `3045111.622 ops/s` | `3060291.466 ops/s` | `+0.50%` | `neutral` |
| `segment-index-hot-partition-put:putThenGetHotPartition` | `1615869.087 ops/s` | `1424431.791 ops/s` | `-11.85%` | `worse` |
| `segment-index-lifecycle:openAndCheckAndRepairConsistency` | `249.065 ms/op` | `249.998 ms/op` | `+0.37%` | `neutral` |
| `segment-index-lifecycle:openAndCompact` | `271.226 ms/op` | `266.123 ms/op` | `-1.88%` | `neutral` |
| `segment-index-lifecycle:openExisting` | `245.407 ms/op` | `243.906 ms/op` | `-0.61%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed` | `515716.093 ops/s` | `500843.266 ops/s` | `-2.88%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed:getWorkload` | `510367.992 ops/s` | `495492.157 ops/s` | `-2.91%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed:putWorkload` | `5348.100 ops/s` | `5351.109 ops/s` | `+0.06%` | `neutral` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed` | `260926.705 ops/s` | `256151.297 ops/s` | `-1.83%` | `neutral` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:getWorkload` | `259539.092 ops/s` | `254614.152 ops/s` | `-1.90%` | `neutral` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:putWorkload` | `1387.614 ops/s` | `1537.146 ops/s` | `+10.78%` | `better` |
| `segment-index-persisted-mutation:deleteAsyncJoin` | `2107.942 ops/s` | `2000.882 ops/s` | `-5.08%` | `warning` |
| `segment-index-persisted-mutation:deleteSync` | `2373.561 ops/s` | `2230.224 ops/s` | `-6.04%` | `warning` |
| `segment-index-persisted-mutation:putAsyncJoin` | `2094.085 ops/s` | `1969.908 ops/s` | `-5.93%` | `warning` |
| `segment-index-persisted-mutation:putSync` | `2289.329 ops/s` | `2169.378 ops/s` | `-5.24%` | `warning` |
| `single-chunk-entry-write-compact:writeEntrySteadyState` | `8371948.028 ops/s` | `8480398.023 ops/s` | `+1.30%` | `neutral` |
| `single-chunk-entry-write-large:writeEntrySteadyState` | `7933429.871 ops/s` | `7850816.189 ops/s` | `-1.04%` | `neutral` |
| `sorted-data-diff-key-read-compact:readNextKey` | `9429747.662 ops/s` | `9140482.694 ops/s` | `-3.07%` | `warning` |
| `sorted-data-diff-key-read-large:readNextKey` | `6781094.972 ops/s` | `7377788.122 ops/s` | `+8.80%` | `better` |
