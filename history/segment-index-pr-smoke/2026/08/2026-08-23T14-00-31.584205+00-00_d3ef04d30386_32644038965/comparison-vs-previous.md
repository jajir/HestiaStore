# Benchmark Comparison

- Profile: `segment-index-pr-smoke`
- Baseline SHA: `6ba0871792734698e0f788c02cc5eadf23ed5d65`
- Candidate SHA: `d3ef04d30386ff556237923d24a98745e3710b30`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `segment-index-get-live:getHitSync` | `4969144.026 ops/s` | `4707024.508 ops/s` | `-5.27%` | `warning` |
| `segment-index-get-live:getMissSync` | `4386179.172 ops/s` | `4495963.676 ops/s` | `+2.50%` | `neutral` |
| `segment-index-get-multisegment-hot:getHitSync` | `3362674.691 ops/s` | `3505546.177 ops/s` | `+4.25%` | `better` |
| `segment-index-get-multisegment-hot:getMissSync` | `4620219.782 ops/s` | `4467706.587 ops/s` | `-3.30%` | `warning` |
| `segment-index-get-persisted:getHitSync` | `3327735.634 ops/s` | `3216912.159 ops/s` | `-3.33%` | `warning` |
| `segment-index-get-persisted:getMissSync` | `4543708.340 ops/s` | `4300515.598 ops/s` | `-5.35%` | `warning` |
| `segment-index-hot-route-put:putHotRoute` | `3918903.550 ops/s` | `3913103.347 ops/s` | `-0.15%` | `neutral` |
| `segment-index-hot-route-put:putThenGetHotRoute` | `2076680.733 ops/s` | `2027743.124 ops/s` | `-2.36%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed` | `581134.535 ops/s` | `559248.321 ops/s` | `-3.77%` | `warning` |
| `segment-index-mixed-drain:partitionedIngestMixed:getWorkload` | `399639.738 ops/s` | `378200.966 ops/s` | `-5.36%` | `warning` |
| `segment-index-mixed-drain:partitionedIngestMixed:putWorkload` | `181494.797 ops/s` | `181047.356 ops/s` | `-0.25%` | `neutral` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed` | `952536.649 ops/s` | `783679.960 ops/s` | `-17.73%` | `worse` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:getWorkload` | `931688.334 ops/s` | `764857.253 ops/s` | `-17.91%` | `worse` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:putWorkload` | `20848.315 ops/s` | `18822.706 ops/s` | `-9.72%` | `worse` |
| `segment-index-persisted-mutation-concurrent:deleteSync` | `7805.742 ops/s` | `7762.050 ops/s` | `-0.56%` | `neutral` |
| `segment-index-persisted-mutation-concurrent:putSync` | `7779.312 ops/s` | `7624.602 ops/s` | `-1.99%` | `neutral` |
| `segment-index-persisted-mutation:deleteSync` | `3353.603 ops/s` | `3573.011 ops/s` | `+6.54%` | `better` |
| `segment-index-persisted-mutation:putSync` | `3564.405 ops/s` | `3552.623 ops/s` | `-0.33%` | `neutral` |
| `segment-index-range-scan:boundedScan` | `30.065 us/op` | `34.903 us/op` | `+16.09%` | `better` |
| `segment-index-range-scan:fullStreamRangeFallback` | `2179.389 us/op` | `2141.080 us/op` | `-1.76%` | `neutral` |
| `segment-index-range-scan:sequentialRead` | `4198.916 us/op` | `4329.613 us/op` | `+3.11%` | `better` |
| `segment-merge-sequential:mergeSequential` | `349.508 us/op` | `349.043 us/op` | `-0.13%` | `neutral` |
