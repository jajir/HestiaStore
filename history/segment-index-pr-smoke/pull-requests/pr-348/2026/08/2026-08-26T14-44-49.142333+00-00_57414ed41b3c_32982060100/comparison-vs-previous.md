# Benchmark Comparison

- Profile: `segment-index-pr-smoke`
- Baseline SHA: `d3ef04d30386ff556237923d24a98745e3710b30`
- Candidate SHA: `57414ed41b3c11fb796684f2415b47efdea3e90a`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `segment-index-get-live:getHitSync` | `4707024.508 ops/s` | `4697923.470 ops/s` | `-0.19%` | `neutral` |
| `segment-index-get-live:getMissSync` | `4495963.676 ops/s` | `4545709.037 ops/s` | `+1.11%` | `neutral` |
| `segment-index-get-multisegment-hot:getHitSync` | `3505546.177 ops/s` | `3490127.965 ops/s` | `-0.44%` | `neutral` |
| `segment-index-get-multisegment-hot:getMissSync` | `4467706.587 ops/s` | `4445773.580 ops/s` | `-0.49%` | `neutral` |
| `segment-index-get-persisted:getHitSync` | `3216912.159 ops/s` | `3305825.160 ops/s` | `+2.76%` | `neutral` |
| `segment-index-get-persisted:getMissSync` | `4300515.598 ops/s` | `4778548.506 ops/s` | `+11.12%` | `better` |
| `segment-index-hot-route-put:putHotRoute` | `3913103.347 ops/s` | `3886345.342 ops/s` | `-0.68%` | `neutral` |
| `segment-index-hot-route-put:putThenGetHotRoute` | `2027743.124 ops/s` | `2054841.327 ops/s` | `+1.34%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed` | `559248.321 ops/s` | `591872.379 ops/s` | `+5.83%` | `better` |
| `segment-index-mixed-drain:partitionedIngestMixed:getWorkload` | `378200.966 ops/s` | `421807.561 ops/s` | `+11.53%` | `better` |
| `segment-index-mixed-drain:partitionedIngestMixed:putWorkload` | `181047.356 ops/s` | `170064.818 ops/s` | `-6.07%` | `warning` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed` | `783679.960 ops/s` | `670119.933 ops/s` | `-14.49%` | `worse` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:getWorkload` | `764857.253 ops/s` | `651176.662 ops/s` | `-14.86%` | `worse` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:putWorkload` | `18822.706 ops/s` | `18943.270 ops/s` | `+0.64%` | `neutral` |
| `segment-index-persisted-mutation-concurrent:deleteSync` | `7762.050 ops/s` | `7640.503 ops/s` | `-1.57%` | `neutral` |
| `segment-index-persisted-mutation-concurrent:putSync` | `7624.602 ops/s` | `7639.419 ops/s` | `+0.19%` | `neutral` |
| `segment-index-persisted-mutation:deleteSync` | `3573.011 ops/s` | `3577.611 ops/s` | `+0.13%` | `neutral` |
| `segment-index-persisted-mutation:putSync` | `3552.623 ops/s` | `3523.120 ops/s` | `-0.83%` | `neutral` |
| `segment-index-range-scan:boundedScan` | `34.903 us/op` | `42.693 us/op` | `+22.32%` | `better` |
| `segment-index-range-scan:fullStreamRangeFallback` | `2141.080 us/op` | `2114.500 us/op` | `-1.24%` | `neutral` |
| `segment-index-range-scan:sequentialRead` | `4329.613 us/op` | `4316.089 us/op` | `-0.31%` | `neutral` |
| `segment-merge-sequential:mergeSequential` | `349.043 us/op` | `350.298 us/op` | `+0.36%` | `neutral` |
