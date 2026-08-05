# Benchmark Comparison

- Profile: `segment-index-nightly`
- Baseline SHA: `99f6033423f943fa22655f00429ed039230292dc`
- Candidate SHA: `99f6033423f943fa22655f00429ed039230292dc`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `segment-index-get-live:getHitSync` | `5256399.071 ops/s` | `5035513.717 ops/s` | `-4.20%` | `warning` |
| `segment-index-get-live:getMissSync` | `4156880.564 ops/s` | `4480301.133 ops/s` | `+7.78%` | `better` |
| `segment-index-get-multisegment-cold:getHitSync` | `272607.849 ops/s` | `276370.183 ops/s` | `+1.38%` | `neutral` |
| `segment-index-get-multisegment-cold:getMissSync` | `4667254.437 ops/s` | `4501690.648 ops/s` | `-3.55%` | `warning` |
| `segment-index-get-multisegment-hot:getHitSync` | `3319688.023 ops/s` | `3358085.276 ops/s` | `+1.16%` | `neutral` |
| `segment-index-get-multisegment-hot:getMissSync` | `4300345.164 ops/s` | `4168120.901 ops/s` | `-3.07%` | `warning` |
| `segment-index-get-persisted:getHitSync` | `3639969.387 ops/s` | `3182150.061 ops/s` | `-12.58%` | `worse` |
| `segment-index-get-persisted:getMissSync` | `4651373.856 ops/s` | `4316241.151 ops/s` | `-7.21%` | `worse` |
| `segment-index-hot-route-put:putHotRoute` | `4074274.817 ops/s` | `3846859.000 ops/s` | `-5.58%` | `warning` |
| `segment-index-hot-route-put:putThenGetHotRoute` | `2226617.538 ops/s` | `1947709.018 ops/s` | `-12.53%` | `worse` |
| `segment-index-lifecycle:openAndCheckAndRepairConsistency` | `309.732 ms/op` | `277.114 ms/op` | `-10.53%` | `worse` |
| `segment-index-lifecycle:openAndCompact` | `332.062 ms/op` | `302.488 ms/op` | `-8.91%` | `worse` |
| `segment-index-lifecycle:openExisting` | `303.755 ms/op` | `273.909 ms/op` | `-9.83%` | `worse` |
| `segment-index-mixed-drain:partitionedIngestMixed` | `523333.219 ops/s` | `509139.484 ops/s` | `-2.71%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed:getWorkload` | `268060.963 ops/s` | `261870.262 ops/s` | `-2.31%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed:putWorkload` | `255272.256 ops/s` | `247269.221 ops/s` | `-3.14%` | `warning` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed` | `1166581.679 ops/s` | `1222512.600 ops/s` | `+4.79%` | `better` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:getWorkload` | `1151027.534 ops/s` | `1207880.485 ops/s` | `+4.94%` | `better` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:putWorkload` | `15554.146 ops/s` | `14632.116 ops/s` | `-5.93%` | `warning` |
| `segment-index-persisted-mutation-concurrent:deleteSync` | `7382.096 ops/s` | `7359.222 ops/s` | `-0.31%` | `neutral` |
| `segment-index-persisted-mutation-concurrent:putSync` | `7361.568 ops/s` | `7618.728 ops/s` | `+3.49%` | `better` |
| `segment-index-persisted-mutation:deleteSync` | `2996.264 ops/s` | `2858.586 ops/s` | `-4.59%` | `warning` |
| `segment-index-persisted-mutation:putSync` | `2841.472 ops/s` | `2777.691 ops/s` | `-2.24%` | `neutral` |
| `segment-index-range-scan:boundedScan` | `32.040 us/op` | `35.998 us/op` | `+12.36%` | `better` |
| `segment-index-range-scan:fullStreamRangeFallback` | `2208.824 us/op` | `2153.581 us/op` | `-2.50%` | `neutral` |
| `segment-index-range-scan:sequentialRead` | `4251.827 us/op` | `4209.077 us/op` | `-1.01%` | `neutral` |
| `segment-merge-sequential:mergeSequential` | `358.418 us/op` | `366.638 us/op` | `+2.29%` | `neutral` |
