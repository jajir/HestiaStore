# Benchmark Comparison

- Candidate labels rerun 3x total and median-merged because the initial regression exceeded threshold with high relative score error: `segment-index-lifecycle,segment-index-mixed-split-heavy`


- Profile: `segment-index-nightly`
- Baseline SHA: `e72f4974ea597aa48d2725b131d13e39bcfa6872`
- Candidate SHA: `e72f4974ea597aa48d2725b131d13e39bcfa6872`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `segment-index-get-multisegment-cold:getHitAsyncJoin` | `41.430 ops/s` | `48.608 ops/s` | `+17.33%` | `better` |
| `segment-index-get-multisegment-cold:getHitSync` | `38.603 ops/s` | `37.149 ops/s` | `-3.77%` | `warning` |
| `segment-index-get-multisegment-cold:getMissAsyncJoin` | `181964.786 ops/s` | `236248.790 ops/s` | `+29.83%` | `better` |
| `segment-index-get-multisegment-cold:getMissSync` | `3733206.985 ops/s` | `2489459.643 ops/s` | `-33.32%` | `worse` |
| `segment-index-get-multisegment-hot:getHitAsyncJoin` | `96.101 ops/s` | `91.408 ops/s` | `-4.88%` | `warning` |
| `segment-index-get-multisegment-hot:getHitSync` | `86.858 ops/s` | `105.137 ops/s` | `+21.04%` | `better` |
| `segment-index-get-multisegment-hot:getMissAsyncJoin` | `184106.643 ops/s` | `236260.158 ops/s` | `+28.33%` | `better` |
| `segment-index-get-multisegment-hot:getMissSync` | `3903430.116 ops/s` | `2492326.174 ops/s` | `-36.15%` | `worse` |
| `segment-index-get-overlay:getHitAsyncJoin` | `174625.870 ops/s` | `217476.996 ops/s` | `+24.54%` | `better` |
| `segment-index-get-overlay:getHitSync` | `4291872.083 ops/s` | `2721205.073 ops/s` | `-36.60%` | `worse` |
| `segment-index-get-overlay:getMissAsyncJoin` | `184954.936 ops/s` | `236062.216 ops/s` | `+27.63%` | `better` |
| `segment-index-get-overlay:getMissSync` | `3526940.837 ops/s` | `2505287.354 ops/s` | `-28.97%` | `worse` |
| `segment-index-get-persisted:getHitAsyncJoin` | `64430.058 ops/s` | `72395.838 ops/s` | `+12.36%` | `better` |
| `segment-index-get-persisted:getHitSync` | `111002.009 ops/s` | `112371.681 ops/s` | `+1.23%` | `neutral` |
| `segment-index-get-persisted:getMissAsyncJoin` | `182890.107 ops/s` | `237610.647 ops/s` | `+29.92%` | `better` |
| `segment-index-get-persisted:getMissSync` | `3984677.846 ops/s` | `2528617.281 ops/s` | `-36.54%` | `worse` |
| `segment-index-hot-partition-put:putHotPartition` | `2835355.140 ops/s` | `2045642.506 ops/s` | `-27.85%` | `worse` |
| `segment-index-hot-partition-put:putThenGetHotPartition` | `1616612.314 ops/s` | `1096847.428 ops/s` | `-32.15%` | `worse` |
| `segment-index-lifecycle:openAndCheckAndRepairConsistency` | `280.252 ms/op` | `135.494 ms/op` | `-51.65%` | `worse` |
| `segment-index-lifecycle:openAndCompact` | `305.329 ms/op` | `157.683 ms/op` | `-48.36%` | `worse` |
| `segment-index-lifecycle:openExisting` | `278.202 ms/op` | `133.417 ms/op` | `-52.04%` | `worse` |
| `segment-index-mixed-drain:partitionedIngestMixed` | `515632.314 ops/s` | `476357.166 ops/s` | `-7.62%` | `worse` |
| `segment-index-mixed-drain:partitionedIngestMixed:getWorkload` | `510207.704 ops/s` | `470994.262 ops/s` | `-7.69%` | `worse` |
| `segment-index-mixed-drain:partitionedIngestMixed:putWorkload` | `5424.610 ops/s` | `5362.904 ops/s` | `-1.14%` | `neutral` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed` | `282822.267 ops/s` | `258095.529 ops/s` | `-8.74%` | `worse` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:getWorkload` | `267123.804 ops/s` | `256770.022 ops/s` | `-3.88%` | `warning` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:putWorkload` | `15698.463 ops/s` | `1427.557 ops/s` | `-90.91%` | `worse` |
| `segment-index-persisted-mutation:deleteAsyncJoin` | `2340.826 ops/s` | `2543.469 ops/s` | `+8.66%` | `better` |
| `segment-index-persisted-mutation:deleteSync` | `2632.791 ops/s` | `2775.428 ops/s` | `+5.42%` | `better` |
| `segment-index-persisted-mutation:putAsyncJoin` | `2363.886 ops/s` | `2504.046 ops/s` | `+5.93%` | `better` |
| `segment-index-persisted-mutation:putSync` | `2618.687 ops/s` | `2728.041 ops/s` | `+4.18%` | `better` |
| `single-chunk-entry-write-compact:writeEntrySteadyState` | `8213557.986 ops/s` | `8884802.183 ops/s` | `+8.17%` | `better` |
| `single-chunk-entry-write-large:writeEntrySteadyState` | `7739893.039 ops/s` | `8066449.338 ops/s` | `+4.22%` | `better` |
| `sorted-data-diff-key-read-compact:readNextKey` | `8577483.454 ops/s` | `8232653.141 ops/s` | `-4.02%` | `warning` |
| `sorted-data-diff-key-read-large:readNextKey` | `6762004.982 ops/s` | `7034070.333 ops/s` | `+4.02%` | `better` |
