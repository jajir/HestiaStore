# Benchmark Comparison

- Profile: `segment-index-nightly`
- Baseline SHA: `d3ef04d30386ff556237923d24a98745e3710b30`
- Candidate SHA: `d3ef04d30386ff556237923d24a98745e3710b30`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `segment-index-get-live:getHitSync` | `5541133.745 ops/s` | `4999366.306 ops/s` | `-9.78%` | `worse` |
| `segment-index-get-live:getMissSync` | `4503061.362 ops/s` | `4685847.911 ops/s` | `+4.06%` | `better` |
| `segment-index-get-multisegment-cold:getHitSync` | `277052.856 ops/s` | `269310.898 ops/s` | `-2.79%` | `neutral` |
| `segment-index-get-multisegment-cold:getMissSync` | `4592319.531 ops/s` | `4473987.817 ops/s` | `-2.58%` | `neutral` |
| `segment-index-get-multisegment-hot:getHitSync` | `3531374.692 ops/s` | `3320817.560 ops/s` | `-5.96%` | `warning` |
| `segment-index-get-multisegment-hot:getMissSync` | `4252990.382 ops/s` | `4365724.705 ops/s` | `+2.65%` | `neutral` |
| `segment-index-get-persisted:getHitSync` | `3379933.278 ops/s` | `3374183.807 ops/s` | `-0.17%` | `neutral` |
| `segment-index-get-persisted:getMissSync` | `4864101.458 ops/s` | `4598536.349 ops/s` | `-5.46%` | `warning` |
| `segment-index-hot-route-put:putHotRoute` | `4372003.459 ops/s` | `4098935.396 ops/s` | `-6.25%` | `warning` |
| `segment-index-hot-route-put:putThenGetHotRoute` | `2695500.458 ops/s` | `2092869.215 ops/s` | `-22.36%` | `worse` |
| `segment-index-lifecycle:openAndCheckAndRepairConsistency` | `243.824 ms/op` | `276.949 ms/op` | `+13.59%` | `better` |
| `segment-index-lifecycle:openAndCompact` | `266.861 ms/op` | `299.096 ms/op` | `+12.08%` | `better` |
| `segment-index-lifecycle:openExisting` | `240.440 ms/op` | `276.153 ms/op` | `+14.85%` | `better` |
| `segment-index-mixed-drain:partitionedIngestMixed` | `524671.310 ops/s` | `566686.151 ops/s` | `+8.01%` | `better` |
| `segment-index-mixed-drain:partitionedIngestMixed:getWorkload` | `249291.856 ops/s` | `292374.218 ops/s` | `+17.28%` | `better` |
| `segment-index-mixed-drain:partitionedIngestMixed:putWorkload` | `275379.453 ops/s` | `274311.934 ops/s` | `-0.39%` | `neutral` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed` | `1271070.192 ops/s` | `1311005.669 ops/s` | `+3.14%` | `better` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:getWorkload` | `1250024.625 ops/s` | `1290810.693 ops/s` | `+3.26%` | `better` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:putWorkload` | `21045.567 ops/s` | `20194.977 ops/s` | `-4.04%` | `warning` |
| `segment-index-persisted-mutation-concurrent:deleteSync` | `7258.915 ops/s` | `7739.255 ops/s` | `+6.62%` | `better` |
| `segment-index-persisted-mutation-concurrent:putSync` | `7262.453 ops/s` | `7762.686 ops/s` | `+6.89%` | `better` |
| `segment-index-persisted-mutation:deleteSync` | `2486.442 ops/s` | `3091.096 ops/s` | `+24.32%` | `better` |
| `segment-index-persisted-mutation:putSync` | `2428.441 ops/s` | `3027.883 ops/s` | `+24.68%` | `better` |
| `segment-index-range-scan:boundedScan` | `29.855 us/op` | `31.146 us/op` | `+4.32%` | `better` |
| `segment-index-range-scan:fullStreamRangeFallback` | `1998.329 us/op` | `2111.974 us/op` | `+5.69%` | `better` |
| `segment-index-range-scan:sequentialRead` | `3745.913 us/op` | `4245.295 us/op` | `+13.33%` | `better` |
| `segment-merge-sequential:mergeSequential` | `333.656 us/op` | `358.379 us/op` | `+7.41%` | `better` |
