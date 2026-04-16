# Benchmark Comparison

- Candidate labels rerun 3x total and median-merged because the initial regression exceeded threshold with high relative score error: `segment-index-persisted-mutation`


- Profile: `segment-index-nightly`
- Baseline SHA: `e72f4974ea597aa48d2725b131d13e39bcfa6872`
- Candidate SHA: `e72f4974ea597aa48d2725b131d13e39bcfa6872`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `segment-index-get-multisegment-cold:getHitAsyncJoin` | `48.998 ops/s` | `46.747 ops/s` | `-4.59%` | `warning` |
| `segment-index-get-multisegment-cold:getHitSync` | `47.364 ops/s` | `46.309 ops/s` | `-2.23%` | `neutral` |
| `segment-index-get-multisegment-cold:getMissAsyncJoin` | `236959.969 ops/s` | `252594.922 ops/s` | `+6.60%` | `better` |
| `segment-index-get-multisegment-cold:getMissSync` | `2380892.700 ops/s` | `4225345.394 ops/s` | `+77.47%` | `better` |
| `segment-index-get-multisegment-hot:getHitAsyncJoin` | `103.565 ops/s` | `100.120 ops/s` | `-3.33%` | `warning` |
| `segment-index-get-multisegment-hot:getHitSync` | `96.472 ops/s` | `102.973 ops/s` | `+6.74%` | `better` |
| `segment-index-get-multisegment-hot:getMissAsyncJoin` | `235029.990 ops/s` | `247111.677 ops/s` | `+5.14%` | `better` |
| `segment-index-get-multisegment-hot:getMissSync` | `2220255.463 ops/s` | `4400677.077 ops/s` | `+98.21%` | `better` |
| `segment-index-get-overlay:getHitAsyncJoin` | `230294.214 ops/s` | `246831.846 ops/s` | `+7.18%` | `better` |
| `segment-index-get-overlay:getHitSync` | `2845167.783 ops/s` | `4883172.720 ops/s` | `+71.63%` | `better` |
| `segment-index-get-overlay:getMissAsyncJoin` | `229681.991 ops/s` | `249613.071 ops/s` | `+8.68%` | `better` |
| `segment-index-get-overlay:getMissSync` | `2351117.875 ops/s` | `4438311.012 ops/s` | `+88.77%` | `better` |
| `segment-index-get-persisted:getHitAsyncJoin` | `72476.642 ops/s` | `76649.574 ops/s` | `+5.76%` | `better` |
| `segment-index-get-persisted:getHitSync` | `108068.401 ops/s` | `162695.036 ops/s` | `+50.55%` | `better` |
| `segment-index-get-persisted:getMissAsyncJoin` | `220713.573 ops/s` | `242806.264 ops/s` | `+10.01%` | `better` |
| `segment-index-get-persisted:getMissSync` | `2468565.884 ops/s` | `4956244.590 ops/s` | `+100.77%` | `better` |
| `segment-index-hot-partition-put:putHotPartition` | `1989636.437 ops/s` | `3640936.331 ops/s` | `+83.00%` | `better` |
| `segment-index-hot-partition-put:putThenGetHotPartition` | `1009209.819 ops/s` | `2110259.985 ops/s` | `+109.10%` | `better` |
| `segment-index-lifecycle:openAndCheckAndRepairConsistency` | `132.644 ms/op` | `217.451 ms/op` | `+63.94%` | `better` |
| `segment-index-lifecycle:openAndCompact` | `155.719 ms/op` | `233.463 ms/op` | `+49.93%` | `better` |
| `segment-index-lifecycle:openExisting` | `132.443 ms/op` | `213.872 ms/op` | `+61.48%` | `better` |
| `segment-index-mixed-drain:partitionedIngestMixed` | `466434.660 ops/s` | `763013.094 ops/s` | `+63.58%` | `better` |
| `segment-index-mixed-drain:partitionedIngestMixed:getWorkload` | `461100.972 ops/s` | `757679.685 ops/s` | `+64.32%` | `better` |
| `segment-index-mixed-drain:partitionedIngestMixed:putWorkload` | `5333.689 ops/s` | `5333.410 ops/s` | `-0.01%` | `neutral` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed` | `247326.274 ops/s` | `382477.972 ops/s` | `+54.65%` | `better` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:getWorkload` | `245908.989 ops/s` | `380847.032 ops/s` | `+54.87%` | `better` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:putWorkload` | `1417.285 ops/s` | `1630.939 ops/s` | `+15.07%` | `better` |
| `segment-index-persisted-mutation:deleteAsyncJoin` | `2516.316 ops/s` | `506.350 ops/s` | `-79.88%` | `worse` |
| `segment-index-persisted-mutation:deleteSync` | `2794.119 ops/s` | `1157.890 ops/s` | `-58.56%` | `worse` |
| `segment-index-persisted-mutation:putAsyncJoin` | `2445.476 ops/s` | `551.408 ops/s` | `-77.45%` | `worse` |
| `segment-index-persisted-mutation:putSync` | `2734.499 ops/s` | `918.227 ops/s` | `-66.42%` | `worse` |
| `single-chunk-entry-write-compact:writeEntrySteadyState` | `8899094.954 ops/s` | `11117649.717 ops/s` | `+24.93%` | `better` |
| `single-chunk-entry-write-large:writeEntrySteadyState` | `8057064.537 ops/s` | `9988087.929 ops/s` | `+23.97%` | `better` |
| `sorted-data-diff-key-read-compact:readNextKey` | `8167049.148 ops/s` | `10605451.550 ops/s` | `+29.86%` | `better` |
| `sorted-data-diff-key-read-large:readNextKey` | `7052356.101 ops/s` | `8084496.483 ops/s` | `+14.64%` | `better` |
