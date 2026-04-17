# Benchmark Comparison

- Candidate labels rerun 3x total and median-merged because the initial regression exceeded threshold with high relative score error: `segment-index-get-multisegment-cold,segment-index-get-multisegment-hot,segment-index-get-persisted,segment-index-mixed-split-heavy`


- Profile: `segment-index-nightly`
- Baseline SHA: `e72f4974ea597aa48d2725b131d13e39bcfa6872`
- Candidate SHA: `e72f4974ea597aa48d2725b131d13e39bcfa6872`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `segment-index-get-multisegment-cold:getHitAsyncJoin` | `46.747 ops/s` | `46.207 ops/s` | `-1.15%` | `neutral` |
| `segment-index-get-multisegment-cold:getHitSync` | `46.309 ops/s` | `49.033 ops/s` | `+5.88%` | `better` |
| `segment-index-get-multisegment-cold:getMissAsyncJoin` | `252594.922 ops/s` | `173028.265 ops/s` | `-31.50%` | `worse` |
| `segment-index-get-multisegment-cold:getMissSync` | `4225345.394 ops/s` | `3719401.049 ops/s` | `-11.97%` | `worse` |
| `segment-index-get-multisegment-hot:getHitAsyncJoin` | `100.120 ops/s` | `96.037 ops/s` | `-4.08%` | `warning` |
| `segment-index-get-multisegment-hot:getHitSync` | `102.973 ops/s` | `93.975 ops/s` | `-8.74%` | `worse` |
| `segment-index-get-multisegment-hot:getMissAsyncJoin` | `247111.677 ops/s` | `171499.878 ops/s` | `-30.60%` | `worse` |
| `segment-index-get-multisegment-hot:getMissSync` | `4400677.077 ops/s` | `3795984.129 ops/s` | `-13.74%` | `worse` |
| `segment-index-get-overlay:getHitAsyncJoin` | `246831.846 ops/s` | `165161.459 ops/s` | `-33.09%` | `worse` |
| `segment-index-get-overlay:getHitSync` | `4883172.720 ops/s` | `3746814.891 ops/s` | `-23.27%` | `worse` |
| `segment-index-get-overlay:getMissAsyncJoin` | `249613.071 ops/s` | `167178.953 ops/s` | `-33.02%` | `worse` |
| `segment-index-get-overlay:getMissSync` | `4438311.012 ops/s` | `3690014.655 ops/s` | `-16.86%` | `worse` |
| `segment-index-get-persisted:getHitAsyncJoin` | `76649.574 ops/s` | `61800.741 ops/s` | `-19.37%` | `worse` |
| `segment-index-get-persisted:getHitSync` | `162695.036 ops/s` | `112452.479 ops/s` | `-30.88%` | `worse` |
| `segment-index-get-persisted:getMissAsyncJoin` | `242806.264 ops/s` | `166593.759 ops/s` | `-31.39%` | `worse` |
| `segment-index-get-persisted:getMissSync` | `4956244.590 ops/s` | `3660380.911 ops/s` | `-26.15%` | `worse` |
| `segment-index-hot-partition-put:putHotPartition` | `3640936.331 ops/s` | `3141585.590 ops/s` | `-13.71%` | `worse` |
| `segment-index-hot-partition-put:putThenGetHotPartition` | `2110259.985 ops/s` | `1606593.481 ops/s` | `-23.87%` | `worse` |
| `segment-index-lifecycle:openAndCheckAndRepairConsistency` | `217.451 ms/op` | `242.731 ms/op` | `+11.63%` | `better` |
| `segment-index-lifecycle:openAndCompact` | `233.463 ms/op` | `264.175 ms/op` | `+13.16%` | `better` |
| `segment-index-lifecycle:openExisting` | `213.872 ms/op` | `238.847 ms/op` | `+11.68%` | `better` |
| `segment-index-mixed-drain:partitionedIngestMixed` | `763013.094 ops/s` | `554256.099 ops/s` | `-27.36%` | `worse` |
| `segment-index-mixed-drain:partitionedIngestMixed:getWorkload` | `757679.685 ops/s` | `548934.597 ops/s` | `-27.55%` | `worse` |
| `segment-index-mixed-drain:partitionedIngestMixed:putWorkload` | `5333.410 ops/s` | `5321.501 ops/s` | `-0.22%` | `neutral` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed` | `382477.972 ops/s` | `263357.012 ops/s` | `-31.14%` | `worse` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:getWorkload` | `380847.032 ops/s` | `261770.289 ops/s` | `-31.27%` | `worse` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:putWorkload` | `1630.939 ops/s` | `1568.194 ops/s` | `-3.85%` | `warning` |
| `segment-index-persisted-mutation:deleteAsyncJoin` | `506.350 ops/s` | `2011.990 ops/s` | `+297.35%` | `better` |
| `segment-index-persisted-mutation:deleteSync` | `1157.890 ops/s` | `2260.871 ops/s` | `+95.26%` | `better` |
| `segment-index-persisted-mutation:putAsyncJoin` | `551.408 ops/s` | `2001.871 ops/s` | `+263.05%` | `better` |
| `segment-index-persisted-mutation:putSync` | `918.227 ops/s` | `2186.783 ops/s` | `+138.15%` | `better` |
| `single-chunk-entry-write-compact:writeEntrySteadyState` | `11117649.717 ops/s` | `8237689.207 ops/s` | `-25.90%` | `worse` |
| `single-chunk-entry-write-large:writeEntrySteadyState` | `9988087.929 ops/s` | `7857977.032 ops/s` | `-21.33%` | `worse` |
| `sorted-data-diff-key-read-compact:readNextKey` | `10605451.550 ops/s` | `8637481.513 ops/s` | `-18.56%` | `worse` |
| `sorted-data-diff-key-read-large:readNextKey` | `8084496.483 ops/s` | `7508381.256 ops/s` | `-7.13%` | `worse` |
