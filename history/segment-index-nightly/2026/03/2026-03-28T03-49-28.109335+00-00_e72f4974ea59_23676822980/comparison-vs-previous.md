# Benchmark Comparison

- Candidate labels rerun 3x total and median-merged because the initial regression exceeded threshold with high relative score error: `segment-index-get-multisegment-cold,segment-index-get-persisted`


- Profile: `segment-index-nightly`
- Baseline SHA: `e72f4974ea597aa48d2725b131d13e39bcfa6872`
- Candidate SHA: `e72f4974ea597aa48d2725b131d13e39bcfa6872`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `segment-index-get-multisegment-cold:getHitAsyncJoin` | `45.603 ops/s` | `40.442 ops/s` | `-11.32%` | `worse` |
| `segment-index-get-multisegment-cold:getHitSync` | `43.455 ops/s` | `39.899 ops/s` | `-8.18%` | `worse` |
| `segment-index-get-multisegment-cold:getMissAsyncJoin` | `234401.777 ops/s` | `172668.754 ops/s` | `-26.34%` | `worse` |
| `segment-index-get-multisegment-cold:getMissSync` | `2412230.094 ops/s` | `3529537.796 ops/s` | `+46.32%` | `better` |
| `segment-index-get-multisegment-hot:getHitAsyncJoin` | `79.632 ops/s` | `86.016 ops/s` | `+8.02%` | `better` |
| `segment-index-get-multisegment-hot:getHitSync` | `80.601 ops/s` | `86.939 ops/s` | `+7.86%` | `better` |
| `segment-index-get-multisegment-hot:getMissAsyncJoin` | `240276.730 ops/s` | `173572.438 ops/s` | `-27.76%` | `worse` |
| `segment-index-get-multisegment-hot:getMissSync` | `2378836.026 ops/s` | `3705152.794 ops/s` | `+55.75%` | `better` |
| `segment-index-get-overlay:getHitAsyncJoin` | `229597.533 ops/s` | `165068.243 ops/s` | `-28.11%` | `worse` |
| `segment-index-get-overlay:getHitSync` | `2635195.982 ops/s` | `4135409.171 ops/s` | `+56.93%` | `better` |
| `segment-index-get-overlay:getMissAsyncJoin` | `230182.611 ops/s` | `165576.345 ops/s` | `-28.07%` | `worse` |
| `segment-index-get-overlay:getMissSync` | `2493539.507 ops/s` | `3654703.794 ops/s` | `+46.57%` | `better` |
| `segment-index-get-persisted:getHitAsyncJoin` | `78362.925 ops/s` | `65809.027 ops/s` | `-16.02%` | `worse` |
| `segment-index-get-persisted:getHitSync` | `115654.250 ops/s` | `117132.575 ops/s` | `+1.28%` | `neutral` |
| `segment-index-get-persisted:getMissAsyncJoin` | `224705.931 ops/s` | `159974.707 ops/s` | `-28.81%` | `worse` |
| `segment-index-get-persisted:getMissSync` | `2634659.406 ops/s` | `4043931.381 ops/s` | `+53.49%` | `better` |
| `segment-index-hot-partition-put:putHotPartition` | `2332653.196 ops/s` | `3069010.783 ops/s` | `+31.57%` | `better` |
| `segment-index-hot-partition-put:putThenGetHotPartition` | `1096250.760 ops/s` | `1538674.372 ops/s` | `+40.36%` | `better` |
| `segment-index-lifecycle:openAndCheckAndRepairConsistency` | `134.484 ms/op` | `246.028 ms/op` | `+82.94%` | `better` |
| `segment-index-lifecycle:openAndCompact` | `154.215 ms/op` | `265.962 ms/op` | `+72.46%` | `better` |
| `segment-index-lifecycle:openExisting` | `131.158 ms/op` | `241.212 ms/op` | `+83.91%` | `better` |
| `segment-index-mixed-drain:partitionedIngestMixed` | `488028.484 ops/s` | `520241.916 ops/s` | `+6.60%` | `better` |
| `segment-index-mixed-drain:partitionedIngestMixed:getWorkload` | `482669.602 ops/s` | `514938.477 ops/s` | `+6.69%` | `better` |
| `segment-index-mixed-drain:partitionedIngestMixed:putWorkload` | `5358.882 ops/s` | `5303.439 ops/s` | `-1.03%` | `neutral` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed` | `249010.985 ops/s` | `264443.893 ops/s` | `+6.20%` | `better` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:getWorkload` | `247461.544 ops/s` | `262976.243 ops/s` | `+6.27%` | `better` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:putWorkload` | `1549.441 ops/s` | `1467.650 ops/s` | `-5.28%` | `warning` |
| `segment-index-persisted-mutation:deleteAsyncJoin` | `2592.404 ops/s` | `2012.821 ops/s` | `-22.36%` | `worse` |
| `segment-index-persisted-mutation:deleteSync` | `2847.237 ops/s` | `2135.670 ops/s` | `-24.99%` | `worse` |
| `segment-index-persisted-mutation:putAsyncJoin` | `2549.422 ops/s` | `1976.802 ops/s` | `-22.46%` | `worse` |
| `segment-index-persisted-mutation:putSync` | `2769.828 ops/s` | `2101.405 ops/s` | `-24.13%` | `worse` |
| `single-chunk-entry-write-compact:writeEntrySteadyState` | `8944859.804 ops/s` | `8424501.287 ops/s` | `-5.82%` | `warning` |
| `single-chunk-entry-write-large:writeEntrySteadyState` | `7958325.127 ops/s` | `7914288.112 ops/s` | `-0.55%` | `neutral` |
| `sorted-data-diff-key-read-compact:readNextKey` | `8197579.248 ops/s` | `8532913.454 ops/s` | `+4.09%` | `better` |
| `sorted-data-diff-key-read-large:readNextKey` | `7141596.151 ops/s` | `7339270.281 ops/s` | `+2.77%` | `neutral` |
