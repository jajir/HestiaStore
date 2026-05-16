# Benchmark Comparison

- Candidate labels rerun 3x total and median-merged because the initial regression exceeded threshold with high relative score error: `segment-index-get-multisegment-cold,segment-index-get-multisegment-hot`


- Profile: `segment-index-nightly`
- Baseline SHA: `e72f4974ea597aa48d2725b131d13e39bcfa6872`
- Candidate SHA: `e72f4974ea597aa48d2725b131d13e39bcfa6872`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `segment-index-get-multisegment-cold:getHitAsyncJoin` | `46.454 ops/s` | `40.359 ops/s` | `-13.12%` | `worse` |
| `segment-index-get-multisegment-cold:getHitSync` | `54.832 ops/s` | `43.508 ops/s` | `-20.65%` | `worse` |
| `segment-index-get-multisegment-cold:getMissAsyncJoin` | `169495.920 ops/s` | `185135.361 ops/s` | `+9.23%` | `better` |
| `segment-index-get-multisegment-cold:getMissSync` | `3798228.274 ops/s` | `3506222.489 ops/s` | `-7.69%` | `worse` |
| `segment-index-get-multisegment-hot:getHitAsyncJoin` | `110.099 ops/s` | `97.297 ops/s` | `-11.63%` | `worse` |
| `segment-index-get-multisegment-hot:getHitSync` | `77.654 ops/s` | `85.016 ops/s` | `+9.48%` | `better` |
| `segment-index-get-multisegment-hot:getMissAsyncJoin` | `167937.645 ops/s` | `185833.398 ops/s` | `+10.66%` | `better` |
| `segment-index-get-multisegment-hot:getMissSync` | `4031623.979 ops/s` | `3424515.682 ops/s` | `-15.06%` | `worse` |
| `segment-index-get-overlay:getHitAsyncJoin` | `154462.019 ops/s` | `178650.879 ops/s` | `+15.66%` | `better` |
| `segment-index-get-overlay:getHitSync` | `3766668.554 ops/s` | `3595220.355 ops/s` | `-4.55%` | `warning` |
| `segment-index-get-overlay:getMissAsyncJoin` | `154795.300 ops/s` | `180469.238 ops/s` | `+16.59%` | `better` |
| `segment-index-get-overlay:getMissSync` | `3646821.216 ops/s` | `3550731.771 ops/s` | `-2.63%` | `neutral` |
| `segment-index-get-persisted:getHitAsyncJoin` | `60426.549 ops/s` | `65599.284 ops/s` | `+8.56%` | `better` |
| `segment-index-get-persisted:getHitSync` | `115830.255 ops/s` | `112622.175 ops/s` | `-2.77%` | `neutral` |
| `segment-index-get-persisted:getMissAsyncJoin` | `163547.865 ops/s` | `187180.674 ops/s` | `+14.45%` | `better` |
| `segment-index-get-persisted:getMissSync` | `3784183.350 ops/s` | `3659333.045 ops/s` | `-3.30%` | `warning` |
| `segment-index-hot-partition-put:putHotPartition` | `2949210.104 ops/s` | `2627784.297 ops/s` | `-10.90%` | `worse` |
| `segment-index-hot-partition-put:putThenGetHotPartition` | `1664425.941 ops/s` | `1397261.796 ops/s` | `-16.05%` | `worse` |
| `segment-index-lifecycle:openAndCheckAndRepairConsistency` | `247.971 ms/op` | `278.314 ms/op` | `+12.24%` | `better` |
| `segment-index-lifecycle:openAndCompact` | `270.451 ms/op` | `302.536 ms/op` | `+11.86%` | `better` |
| `segment-index-lifecycle:openExisting` | `245.891 ms/op` | `277.993 ms/op` | `+13.06%` | `better` |
| `segment-index-mixed-drain:partitionedIngestMixed` | `512603.681 ops/s` | `509777.250 ops/s` | `-0.55%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed:getWorkload` | `507291.146 ops/s` | `504437.661 ops/s` | `-0.56%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed:putWorkload` | `5312.535 ops/s` | `5339.589 ops/s` | `+0.51%` | `neutral` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed` | `266903.077 ops/s` | `264302.602 ops/s` | `-0.97%` | `neutral` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:getWorkload` | `265421.482 ops/s` | `262674.123 ops/s` | `-1.04%` | `neutral` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:putWorkload` | `1481.595 ops/s` | `1628.479 ops/s` | `+9.91%` | `better` |
| `segment-index-persisted-mutation:deleteAsyncJoin` | `1932.619 ops/s` | `2487.552 ops/s` | `+28.71%` | `better` |
| `segment-index-persisted-mutation:deleteSync` | `2240.396 ops/s` | `2696.353 ops/s` | `+20.35%` | `better` |
| `segment-index-persisted-mutation:putAsyncJoin` | `1944.254 ops/s` | `2368.250 ops/s` | `+21.81%` | `better` |
| `segment-index-persisted-mutation:putSync` | `2176.611 ops/s` | `2595.812 ops/s` | `+19.26%` | `better` |
| `single-chunk-entry-write-compact:writeEntrySteadyState` | `8431583.726 ops/s` | `8300666.202 ops/s` | `-1.55%` | `neutral` |
| `single-chunk-entry-write-large:writeEntrySteadyState` | `7995691.979 ops/s` | `7748587.969 ops/s` | `-3.09%` | `warning` |
| `sorted-data-diff-key-read-compact:readNextKey` | `9435275.787 ops/s` | `8560162.243 ops/s` | `-9.27%` | `worse` |
| `sorted-data-diff-key-read-large:readNextKey` | `7473315.219 ops/s` | `6528436.547 ops/s` | `-12.64%` | `worse` |
