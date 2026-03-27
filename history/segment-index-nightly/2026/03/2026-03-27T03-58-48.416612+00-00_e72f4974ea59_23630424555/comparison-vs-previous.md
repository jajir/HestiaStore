# Benchmark Comparison

- Candidate labels rerun 3x total and median-merged because the initial regression exceeded threshold with high relative score error: `segment-index-get-multisegment-hot,segment-index-mixed-split-heavy`


- Profile: `segment-index-nightly`
- Baseline SHA: `a87e6e844c0f83bfc93f8ff3748153d6ffec3dbb`
- Candidate SHA: `e72f4974ea597aa48d2725b131d13e39bcfa6872`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `segment-index-get-multisegment-cold:getHitAsyncJoin` | `43.070 ops/s` | `45.603 ops/s` | `+5.88%` | `better` |
| `segment-index-get-multisegment-cold:getHitSync` | `40.342 ops/s` | `43.455 ops/s` | `+7.72%` | `better` |
| `segment-index-get-multisegment-cold:getMissAsyncJoin` | `165680.149 ops/s` | `234401.777 ops/s` | `+41.48%` | `better` |
| `segment-index-get-multisegment-cold:getMissSync` | `3777865.175 ops/s` | `2412230.094 ops/s` | `-36.15%` | `worse` |
| `segment-index-get-multisegment-hot:getHitAsyncJoin` | `94.601 ops/s` | `79.632 ops/s` | `-15.82%` | `worse` |
| `segment-index-get-multisegment-hot:getHitSync` | `82.090 ops/s` | `80.601 ops/s` | `-1.81%` | `neutral` |
| `segment-index-get-multisegment-hot:getMissAsyncJoin` | `165842.562 ops/s` | `240276.730 ops/s` | `+44.88%` | `better` |
| `segment-index-get-multisegment-hot:getMissSync` | `3671012.090 ops/s` | `2378836.026 ops/s` | `-35.20%` | `worse` |
| `segment-index-get-overlay:getHitAsyncJoin` | `163076.740 ops/s` | `229597.533 ops/s` | `+40.79%` | `better` |
| `segment-index-get-overlay:getHitSync` | `3895405.023 ops/s` | `2635195.982 ops/s` | `-32.35%` | `worse` |
| `segment-index-get-overlay:getMissAsyncJoin` | `161397.647 ops/s` | `230182.611 ops/s` | `+42.62%` | `better` |
| `segment-index-get-overlay:getMissSync` | `3730332.937 ops/s` | `2493539.507 ops/s` | `-33.16%` | `worse` |
| `segment-index-get-persisted:getHitAsyncJoin` | `61264.844 ops/s` | `78362.925 ops/s` | `+27.91%` | `better` |
| `segment-index-get-persisted:getHitSync` | `109892.906 ops/s` | `115654.250 ops/s` | `+5.24%` | `better` |
| `segment-index-get-persisted:getMissAsyncJoin` | `166466.175 ops/s` | `224705.931 ops/s` | `+34.99%` | `better` |
| `segment-index-get-persisted:getMissSync` | `3822535.042 ops/s` | `2634659.406 ops/s` | `-31.08%` | `worse` |
| `segment-index-hot-partition-put:putHotPartition` | `3068871.333 ops/s` | `2332653.196 ops/s` | `-23.99%` | `worse` |
| `segment-index-hot-partition-put:putThenGetHotPartition` | `1611393.893 ops/s` | `1096250.760 ops/s` | `-31.97%` | `worse` |
| `segment-index-lifecycle:openAndCheckAndRepairConsistency` | `303.261 ms/op` | `134.484 ms/op` | `-55.65%` | `worse` |
| `segment-index-lifecycle:openAndCompact` | `328.243 ms/op` | `154.215 ms/op` | `-53.02%` | `worse` |
| `segment-index-lifecycle:openExisting` | `303.833 ms/op` | `131.158 ms/op` | `-56.83%` | `worse` |
| `segment-index-mixed-drain:partitionedIngestMixed` | `562649.495 ops/s` | `488028.484 ops/s` | `-13.26%` | `worse` |
| `segment-index-mixed-drain:partitionedIngestMixed:getWorkload` | `557299.922 ops/s` | `482669.602 ops/s` | `-13.39%` | `worse` |
| `segment-index-mixed-drain:partitionedIngestMixed:putWorkload` | `5349.573 ops/s` | `5358.882 ops/s` | `+0.17%` | `neutral` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed` | `352124.296 ops/s` | `249010.985 ops/s` | `-29.28%` | `worse` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:getWorkload` | `265339.151 ops/s` | `247461.544 ops/s` | `-6.74%` | `warning` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:putWorkload` | `86785.145 ops/s` | `1549.441 ops/s` | `-98.21%` | `worse` |
| `segment-index-persisted-mutation:deleteAsyncJoin` | `2044.124 ops/s` | `2592.404 ops/s` | `+26.82%` | `better` |
| `segment-index-persisted-mutation:deleteSync` | `2202.991 ops/s` | `2847.237 ops/s` | `+29.24%` | `better` |
| `segment-index-persisted-mutation:putAsyncJoin` | `2020.736 ops/s` | `2549.422 ops/s` | `+26.16%` | `better` |
| `segment-index-persisted-mutation:putSync` | `2160.229 ops/s` | `2769.828 ops/s` | `+28.22%` | `better` |
| `single-chunk-entry-write-compact:writeEntrySteadyState` | `8335392.942 ops/s` | `8944859.804 ops/s` | `+7.31%` | `better` |
| `single-chunk-entry-write-large:writeEntrySteadyState` | `7846180.623 ops/s` | `7958325.127 ops/s` | `+1.43%` | `neutral` |
| `sorted-data-diff-key-read-compact:readNextKey` | `9031196.790 ops/s` | `8197579.248 ops/s` | `-9.23%` | `worse` |
| `sorted-data-diff-key-read-large:readNextKey` | `6818517.595 ops/s` | `7141596.151 ops/s` | `+4.74%` | `better` |
