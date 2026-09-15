# Benchmark Comparison

- Profile: `segment-index-pr-smoke`
- Baseline SHA: `d3ef04d30386ff556237923d24a98745e3710b30`
- Candidate SHA: `9502d73d397c1440a3a93bfd6e7f8a16930ba015`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `segment-index-get-live:getHitSync` | `4707024.508 ops/s` | `2839390.518 ops/s` | `-39.68%` | `worse` |
| `segment-index-get-live:getMissSync` | `4495963.676 ops/s` | `2535092.299 ops/s` | `-43.61%` | `worse` |
| `segment-index-get-multisegment-hot:getHitSync` | `3505546.177 ops/s` | `2328908.944 ops/s` | `-33.57%` | `worse` |
| `segment-index-get-multisegment-hot:getMissSync` | `4467706.587 ops/s` | `2710220.605 ops/s` | `-39.34%` | `worse` |
| `segment-index-get-persisted:getHitSync` | `3216912.159 ops/s` | `2073557.116 ops/s` | `-35.54%` | `worse` |
| `segment-index-get-persisted:getMissSync` | `4300515.598 ops/s` | `2830666.691 ops/s` | `-34.18%` | `worse` |
| `segment-index-hot-route-put:putHotRoute` | `3913103.347 ops/s` | `2892311.515 ops/s` | `-26.09%` | `worse` |
| `segment-index-hot-route-put:putThenGetHotRoute` | `2027743.124 ops/s` | `1415525.460 ops/s` | `-30.19%` | `worse` |
| `segment-index-mixed-drain:partitionedIngestMixed` | `559248.321 ops/s` | `599357.658 ops/s` | `+7.17%` | `better` |
| `segment-index-mixed-drain:partitionedIngestMixed:getWorkload` | `378200.966 ops/s` | `359397.427 ops/s` | `-4.97%` | `warning` |
| `segment-index-mixed-drain:partitionedIngestMixed:putWorkload` | `181047.356 ops/s` | `239960.231 ops/s` | `+32.54%` | `better` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed` | `783679.960 ops/s` | `892107.042 ops/s` | `+13.84%` | `better` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:getWorkload` | `764857.253 ops/s` | `851402.197 ops/s` | `+11.32%` | `better` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:putWorkload` | `18822.706 ops/s` | `40704.845 ops/s` | `+116.25%` | `better` |
| `segment-index-persisted-mutation-concurrent:deleteSync` | `7762.050 ops/s` | `3817.248 ops/s` | `-50.82%` | `worse` |
| `segment-index-persisted-mutation-concurrent:putSync` | `7624.602 ops/s` | `3773.602 ops/s` | `-50.51%` | `worse` |
| `segment-index-persisted-mutation:deleteSync` | `3573.011 ops/s` | `1870.803 ops/s` | `-47.64%` | `worse` |
| `segment-index-persisted-mutation:putSync` | `3552.623 ops/s` | `1808.375 ops/s` | `-49.10%` | `worse` |
| `segment-index-range-scan:boundedScan` | `34.903 us/op` | `20.207 us/op` | `-42.10%` | `worse` |
| `segment-index-range-scan:fullStreamRangeFallback` | `2141.080 us/op` | `1645.942 us/op` | `-23.13%` | `worse` |
| `segment-index-range-scan:sequentialRead` | `4329.613 us/op` | `3188.597 us/op` | `-26.35%` | `worse` |
| `segment-merge-sequential:mergeSequential` | `349.043 us/op` | `258.801 us/op` | `-25.85%` | `worse` |
