# Benchmark Comparison

- Candidate labels rerun 3x total and median-merged because the initial regression exceeded threshold with high relative score error: `segment-index-lifecycle`


- Profile: `segment-index-nightly`
- Baseline SHA: `e72f4974ea597aa48d2725b131d13e39bcfa6872`
- Candidate SHA: `e72f4974ea597aa48d2725b131d13e39bcfa6872`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `segment-index-get-multisegment-cold:getHitAsyncJoin` | `40.359 ops/s` | `46.730 ops/s` | `+15.78%` | `better` |
| `segment-index-get-multisegment-cold:getHitSync` | `43.508 ops/s` | `47.235 ops/s` | `+8.57%` | `better` |
| `segment-index-get-multisegment-cold:getMissAsyncJoin` | `185135.361 ops/s` | `173636.729 ops/s` | `-6.21%` | `warning` |
| `segment-index-get-multisegment-cold:getMissSync` | `3506222.489 ops/s` | `3515044.466 ops/s` | `+0.25%` | `neutral` |
| `segment-index-get-multisegment-hot:getHitAsyncJoin` | `97.297 ops/s` | `123.318 ops/s` | `+26.74%` | `better` |
| `segment-index-get-multisegment-hot:getHitSync` | `85.016 ops/s` | `99.273 ops/s` | `+16.77%` | `better` |
| `segment-index-get-multisegment-hot:getMissAsyncJoin` | `185833.398 ops/s` | `173213.678 ops/s` | `-6.79%` | `warning` |
| `segment-index-get-multisegment-hot:getMissSync` | `3424515.682 ops/s` | `3744586.415 ops/s` | `+9.35%` | `better` |
| `segment-index-get-overlay:getHitAsyncJoin` | `178650.879 ops/s` | `152997.241 ops/s` | `-14.36%` | `worse` |
| `segment-index-get-overlay:getHitSync` | `3595220.355 ops/s` | `4281968.552 ops/s` | `+19.10%` | `better` |
| `segment-index-get-overlay:getMissAsyncJoin` | `180469.238 ops/s` | `160687.259 ops/s` | `-10.96%` | `worse` |
| `segment-index-get-overlay:getMissSync` | `3550731.771 ops/s` | `3752476.737 ops/s` | `+5.68%` | `better` |
| `segment-index-get-persisted:getHitAsyncJoin` | `65599.284 ops/s` | `62421.308 ops/s` | `-4.84%` | `warning` |
| `segment-index-get-persisted:getHitSync` | `112622.175 ops/s` | `115241.311 ops/s` | `+2.33%` | `neutral` |
| `segment-index-get-persisted:getMissAsyncJoin` | `187180.674 ops/s` | `157477.056 ops/s` | `-15.87%` | `worse` |
| `segment-index-get-persisted:getMissSync` | `3659333.045 ops/s` | `3663306.335 ops/s` | `+0.11%` | `neutral` |
| `segment-index-hot-partition-put:putHotPartition` | `2627784.297 ops/s` | `3045111.622 ops/s` | `+15.88%` | `better` |
| `segment-index-hot-partition-put:putThenGetHotPartition` | `1397261.796 ops/s` | `1615869.087 ops/s` | `+15.65%` | `better` |
| `segment-index-lifecycle:openAndCheckAndRepairConsistency` | `278.314 ms/op` | `249.065 ms/op` | `-10.51%` | `worse` |
| `segment-index-lifecycle:openAndCompact` | `302.536 ms/op` | `271.226 ms/op` | `-10.35%` | `worse` |
| `segment-index-lifecycle:openExisting` | `277.993 ms/op` | `245.407 ms/op` | `-11.72%` | `worse` |
| `segment-index-mixed-drain:partitionedIngestMixed` | `509777.250 ops/s` | `515716.093 ops/s` | `+1.16%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed:getWorkload` | `504437.661 ops/s` | `510367.992 ops/s` | `+1.18%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed:putWorkload` | `5339.589 ops/s` | `5348.100 ops/s` | `+0.16%` | `neutral` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed` | `264302.602 ops/s` | `260926.705 ops/s` | `-1.28%` | `neutral` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:getWorkload` | `262674.123 ops/s` | `259539.092 ops/s` | `-1.19%` | `neutral` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:putWorkload` | `1628.479 ops/s` | `1387.614 ops/s` | `-14.79%` | `worse` |
| `segment-index-persisted-mutation:deleteAsyncJoin` | `2487.552 ops/s` | `2107.942 ops/s` | `-15.26%` | `worse` |
| `segment-index-persisted-mutation:deleteSync` | `2696.353 ops/s` | `2373.561 ops/s` | `-11.97%` | `worse` |
| `segment-index-persisted-mutation:putAsyncJoin` | `2368.250 ops/s` | `2094.085 ops/s` | `-11.58%` | `worse` |
| `segment-index-persisted-mutation:putSync` | `2595.812 ops/s` | `2289.329 ops/s` | `-11.81%` | `worse` |
| `single-chunk-entry-write-compact:writeEntrySteadyState` | `8300666.202 ops/s` | `8371948.028 ops/s` | `+0.86%` | `neutral` |
| `single-chunk-entry-write-large:writeEntrySteadyState` | `7748587.969 ops/s` | `7933429.871 ops/s` | `+2.39%` | `neutral` |
| `sorted-data-diff-key-read-compact:readNextKey` | `8560162.243 ops/s` | `9429747.662 ops/s` | `+10.16%` | `better` |
| `sorted-data-diff-key-read-large:readNextKey` | `6528436.547 ops/s` | `6781094.972 ops/s` | `+3.87%` | `better` |
