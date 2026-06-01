# Benchmark Comparison

- Candidate labels rerun 3x total and median-merged because the initial regression exceeded threshold with high relative score error: `segment-index-get-multisegment-cold`


- Profile: `segment-index-nightly`
- Baseline SHA: `e72f4974ea597aa48d2725b131d13e39bcfa6872`
- Candidate SHA: `e72f4974ea597aa48d2725b131d13e39bcfa6872`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `segment-index-get-multisegment-cold:getHitAsyncJoin` | `48.608 ops/s` | `50.477 ops/s` | `+3.85%` | `better` |
| `segment-index-get-multisegment-cold:getHitSync` | `37.149 ops/s` | `48.795 ops/s` | `+31.35%` | `better` |
| `segment-index-get-multisegment-cold:getMissAsyncJoin` | `236248.790 ops/s` | `171546.333 ops/s` | `-27.39%` | `worse` |
| `segment-index-get-multisegment-cold:getMissSync` | `2489459.643 ops/s` | `3504023.683 ops/s` | `+40.75%` | `better` |
| `segment-index-get-multisegment-hot:getHitAsyncJoin` | `91.408 ops/s` | `86.842 ops/s` | `-4.99%` | `warning` |
| `segment-index-get-multisegment-hot:getHitSync` | `105.137 ops/s` | `78.808 ops/s` | `-25.04%` | `worse` |
| `segment-index-get-multisegment-hot:getMissAsyncJoin` | `236260.158 ops/s` | `171997.189 ops/s` | `-27.20%` | `worse` |
| `segment-index-get-multisegment-hot:getMissSync` | `2492326.174 ops/s` | `3634622.857 ops/s` | `+45.83%` | `better` |
| `segment-index-get-overlay:getHitAsyncJoin` | `217476.996 ops/s` | `163750.336 ops/s` | `-24.70%` | `worse` |
| `segment-index-get-overlay:getHitSync` | `2721205.073 ops/s` | `3943900.051 ops/s` | `+44.93%` | `better` |
| `segment-index-get-overlay:getMissAsyncJoin` | `236062.216 ops/s` | `163157.590 ops/s` | `-30.88%` | `worse` |
| `segment-index-get-overlay:getMissSync` | `2505287.354 ops/s` | `3596652.290 ops/s` | `+43.56%` | `better` |
| `segment-index-get-persisted:getHitAsyncJoin` | `72395.838 ops/s` | `59167.213 ops/s` | `-18.27%` | `worse` |
| `segment-index-get-persisted:getHitSync` | `112371.681 ops/s` | `116949.483 ops/s` | `+4.07%` | `better` |
| `segment-index-get-persisted:getMissAsyncJoin` | `237610.647 ops/s` | `164942.156 ops/s` | `-30.58%` | `worse` |
| `segment-index-get-persisted:getMissSync` | `2528617.281 ops/s` | `3914720.510 ops/s` | `+54.82%` | `better` |
| `segment-index-hot-partition-put:putHotPartition` | `2045642.506 ops/s` | `2871119.161 ops/s` | `+40.35%` | `better` |
| `segment-index-hot-partition-put:putThenGetHotPartition` | `1096847.428 ops/s` | `1558568.841 ops/s` | `+42.10%` | `better` |
| `segment-index-lifecycle:openAndCheckAndRepairConsistency` | `135.494 ms/op` | `245.828 ms/op` | `+81.43%` | `better` |
| `segment-index-lifecycle:openAndCompact` | `157.683 ms/op` | `268.390 ms/op` | `+70.21%` | `better` |
| `segment-index-lifecycle:openExisting` | `133.417 ms/op` | `244.120 ms/op` | `+82.98%` | `better` |
| `segment-index-mixed-drain:partitionedIngestMixed` | `476357.166 ops/s` | `503510.722 ops/s` | `+5.70%` | `better` |
| `segment-index-mixed-drain:partitionedIngestMixed:getWorkload` | `470994.262 ops/s` | `498190.291 ops/s` | `+5.77%` | `better` |
| `segment-index-mixed-drain:partitionedIngestMixed:putWorkload` | `5362.904 ops/s` | `5320.431 ops/s` | `-0.79%` | `neutral` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed` | `258095.529 ops/s` | `265329.273 ops/s` | `+2.80%` | `neutral` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:getWorkload` | `256770.022 ops/s` | `263798.759 ops/s` | `+2.74%` | `neutral` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:putWorkload` | `1427.557 ops/s` | `1530.514 ops/s` | `+7.21%` | `better` |
| `segment-index-persisted-mutation:deleteAsyncJoin` | `2543.469 ops/s` | `1996.147 ops/s` | `-21.52%` | `worse` |
| `segment-index-persisted-mutation:deleteSync` | `2775.428 ops/s` | `2219.076 ops/s` | `-20.05%` | `worse` |
| `segment-index-persisted-mutation:putAsyncJoin` | `2504.046 ops/s` | `1969.064 ops/s` | `-21.36%` | `worse` |
| `segment-index-persisted-mutation:putSync` | `2728.041 ops/s` | `2178.491 ops/s` | `-20.14%` | `worse` |
| `single-chunk-entry-write-compact:writeEntrySteadyState` | `8884802.183 ops/s` | `8480858.639 ops/s` | `-4.55%` | `warning` |
| `single-chunk-entry-write-large:writeEntrySteadyState` | `8066449.338 ops/s` | `7945338.216 ops/s` | `-1.50%` | `neutral` |
| `sorted-data-diff-key-read-compact:readNextKey` | `8232653.141 ops/s` | `9047526.119 ops/s` | `+9.90%` | `better` |
| `sorted-data-diff-key-read-large:readNextKey` | `7034070.333 ops/s` | `7445410.546 ops/s` | `+5.85%` | `better` |
