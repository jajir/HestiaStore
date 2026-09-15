# Benchmark Comparison

- Profile: `segment-index-pr-smoke`
- Baseline SHA: `d3ef04d30386ff556237923d24a98745e3710b30`
- Candidate SHA: `6da25f8d2edfcb35fc090cb0bcf177026066b7a3`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `segment-index-get-live:getHitSync` | `4707024.508 ops/s` | `4545773.738 ops/s` | `-3.43%` | `warning` |
| `segment-index-get-live:getMissSync` | `4495963.676 ops/s` | `4523413.683 ops/s` | `+0.61%` | `neutral` |
| `segment-index-get-multisegment-hot:getHitSync` | `3505546.177 ops/s` | `3585813.582 ops/s` | `+2.29%` | `neutral` |
| `segment-index-get-multisegment-hot:getMissSync` | `4467706.587 ops/s` | `4591399.692 ops/s` | `+2.77%` | `neutral` |
| `segment-index-get-persisted:getHitSync` | `3216912.159 ops/s` | `3564622.523 ops/s` | `+10.81%` | `better` |
| `segment-index-get-persisted:getMissSync` | `4300515.598 ops/s` | `4925693.458 ops/s` | `+14.54%` | `better` |
| `segment-index-hot-route-put:putHotRoute` | `3913103.347 ops/s` | `4288631.674 ops/s` | `+9.60%` | `better` |
| `segment-index-hot-route-put:putThenGetHotRoute` | `2027743.124 ops/s` | `2105899.760 ops/s` | `+3.85%` | `better` |
| `segment-index-mixed-drain:partitionedIngestMixed` | `559248.321 ops/s` | `548538.689 ops/s` | `-1.92%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed:getWorkload` | `378200.966 ops/s` | `353472.288 ops/s` | `-6.54%` | `warning` |
| `segment-index-mixed-drain:partitionedIngestMixed:putWorkload` | `181047.356 ops/s` | `195066.401 ops/s` | `+7.74%` | `better` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed` | `783679.960 ops/s` | `762133.644 ops/s` | `-2.75%` | `neutral` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:getWorkload` | `764857.253 ops/s` | `732891.958 ops/s` | `-4.18%` | `warning` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:putWorkload` | `18822.706 ops/s` | `29241.686 ops/s` | `+55.35%` | `better` |
| `segment-index-persisted-mutation-concurrent:deleteSync` | `7762.050 ops/s` | `6466.878 ops/s` | `-16.69%` | `worse` |
| `segment-index-persisted-mutation-concurrent:putSync` | `7624.602 ops/s` | `6596.854 ops/s` | `-13.48%` | `worse` |
| `segment-index-persisted-mutation:deleteSync` | `3573.011 ops/s` | `2641.550 ops/s` | `-26.07%` | `worse` |
| `segment-index-persisted-mutation:putSync` | `3552.623 ops/s` | `2650.243 ops/s` | `-25.40%` | `worse` |
| `segment-index-range-scan:boundedScan` | `34.903 us/op` | `26.668 us/op` | `-23.59%` | `worse` |
| `segment-index-range-scan:fullStreamRangeFallback` | `2141.080 us/op` | `2070.861 us/op` | `-3.28%` | `warning` |
| `segment-index-range-scan:sequentialRead` | `4329.613 us/op` | `4090.204 us/op` | `-5.53%` | `warning` |
| `segment-merge-sequential:mergeSequential` | `349.043 us/op` | `324.248 us/op` | `-7.10%` | `worse` |
