# Benchmark Comparison

- Candidate labels rerun 3x total and median-merged because the initial regression exceeded threshold with high relative score error: `segment-index-get-multisegment-cold,segment-index-lifecycle`


- Profile: `segment-index-nightly`
- Baseline SHA: `e72f4974ea597aa48d2725b131d13e39bcfa6872`
- Candidate SHA: `e72f4974ea597aa48d2725b131d13e39bcfa6872`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `segment-index-get-multisegment-cold:getHitAsyncJoin` | `48.081 ops/s` | `42.731 ops/s` | `-11.13%` | `worse` |
| `segment-index-get-multisegment-cold:getHitSync` | `52.086 ops/s` | `43.654 ops/s` | `-16.19%` | `worse` |
| `segment-index-get-multisegment-cold:getMissAsyncJoin` | `187003.526 ops/s` | `172825.766 ops/s` | `-7.58%` | `worse` |
| `segment-index-get-multisegment-cold:getMissSync` | `3753296.366 ops/s` | `3866871.864 ops/s` | `+3.03%` | `better` |
| `segment-index-get-multisegment-hot:getHitAsyncJoin` | `87.346 ops/s` | `111.368 ops/s` | `+27.50%` | `better` |
| `segment-index-get-multisegment-hot:getHitSync` | `88.785 ops/s` | `91.730 ops/s` | `+3.32%` | `better` |
| `segment-index-get-multisegment-hot:getMissAsyncJoin` | `185469.965 ops/s` | `174292.229 ops/s` | `-6.03%` | `warning` |
| `segment-index-get-multisegment-hot:getMissSync` | `3628332.702 ops/s` | `4033291.669 ops/s` | `+11.16%` | `better` |
| `segment-index-get-overlay:getHitAsyncJoin` | `177982.480 ops/s` | `167423.247 ops/s` | `-5.93%` | `warning` |
| `segment-index-get-overlay:getHitSync` | `3656918.412 ops/s` | `4005659.731 ops/s` | `+9.54%` | `better` |
| `segment-index-get-overlay:getMissAsyncJoin` | `179942.522 ops/s` | `160014.204 ops/s` | `-11.07%` | `worse` |
| `segment-index-get-overlay:getMissSync` | `3690449.583 ops/s` | `3589694.947 ops/s` | `-2.73%` | `neutral` |
| `segment-index-get-persisted:getHitAsyncJoin` | `61081.870 ops/s` | `59382.416 ops/s` | `-2.78%` | `neutral` |
| `segment-index-get-persisted:getHitSync` | `113691.484 ops/s` | `117210.808 ops/s` | `+3.10%` | `better` |
| `segment-index-get-persisted:getMissAsyncJoin` | `179191.292 ops/s` | `162256.601 ops/s` | `-9.45%` | `worse` |
| `segment-index-get-persisted:getMissSync` | `3469891.285 ops/s` | `3599526.147 ops/s` | `+3.74%` | `better` |
| `segment-index-hot-partition-put:putHotPartition` | `2847295.081 ops/s` | `2764822.764 ops/s` | `-2.90%` | `neutral` |
| `segment-index-hot-partition-put:putThenGetHotPartition` | `1400574.519 ops/s` | `1598470.017 ops/s` | `+14.13%` | `better` |
| `segment-index-lifecycle:openAndCheckAndRepairConsistency` | `277.887 ms/op` | `244.800 ms/op` | `-11.91%` | `worse` |
| `segment-index-lifecycle:openAndCompact` | `302.250 ms/op` | `265.766 ms/op` | `-12.07%` | `worse` |
| `segment-index-lifecycle:openExisting` | `275.829 ms/op` | `240.933 ms/op` | `-12.65%` | `worse` |
| `segment-index-mixed-drain:partitionedIngestMixed` | `519676.299 ops/s` | `506189.097 ops/s` | `-2.60%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed:getWorkload` | `514326.855 ops/s` | `500841.249 ops/s` | `-2.62%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed:putWorkload` | `5349.445 ops/s` | `5347.847 ops/s` | `-0.03%` | `neutral` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed` | `259604.522 ops/s` | `262677.107 ops/s` | `+1.18%` | `neutral` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:getWorkload` | `258042.366 ops/s` | `261322.263 ops/s` | `+1.27%` | `neutral` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:putWorkload` | `1562.155 ops/s` | `1354.844 ops/s` | `-13.27%` | `worse` |
| `segment-index-persisted-mutation:deleteAsyncJoin` | `2563.214 ops/s` | `1995.375 ops/s` | `-22.15%` | `worse` |
| `segment-index-persisted-mutation:deleteSync` | `2748.543 ops/s` | `2186.491 ops/s` | `-20.45%` | `worse` |
| `segment-index-persisted-mutation:putAsyncJoin` | `2492.533 ops/s` | `1962.279 ops/s` | `-21.27%` | `worse` |
| `segment-index-persisted-mutation:putSync` | `2751.180 ops/s` | `2072.412 ops/s` | `-24.67%` | `worse` |
| `single-chunk-entry-write-compact:writeEntrySteadyState` | `8227522.645 ops/s` | `8506531.538 ops/s` | `+3.39%` | `better` |
| `single-chunk-entry-write-large:writeEntrySteadyState` | `7753680.497 ops/s` | `7940126.533 ops/s` | `+2.40%` | `neutral` |
| `sorted-data-diff-key-read-compact:readNextKey` | `8471452.008 ops/s` | `9471661.929 ops/s` | `+11.81%` | `better` |
| `sorted-data-diff-key-read-large:readNextKey` | `6389431.842 ops/s` | `7349591.111 ops/s` | `+15.03%` | `better` |
