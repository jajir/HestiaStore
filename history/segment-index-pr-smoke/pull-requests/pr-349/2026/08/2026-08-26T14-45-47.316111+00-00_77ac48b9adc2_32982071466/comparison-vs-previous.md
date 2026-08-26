# Benchmark Comparison

- Profile: `segment-index-pr-smoke`
- Baseline SHA: `d3ef04d30386ff556237923d24a98745e3710b30`
- Candidate SHA: `77ac48b9adc2f1953f8137a8d425c78aab025789`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `segment-index-get-live:getHitSync` | `4707024.508 ops/s` | `5146326.112 ops/s` | `+9.33%` | `better` |
| `segment-index-get-live:getMissSync` | `4495963.676 ops/s` | `4836089.490 ops/s` | `+7.57%` | `better` |
| `segment-index-get-multisegment-hot:getHitSync` | `3505546.177 ops/s` | `3501430.475 ops/s` | `-0.12%` | `neutral` |
| `segment-index-get-multisegment-hot:getMissSync` | `4467706.587 ops/s` | `4913576.138 ops/s` | `+9.98%` | `better` |
| `segment-index-get-persisted:getHitSync` | `3216912.159 ops/s` | `3382101.175 ops/s` | `+5.14%` | `better` |
| `segment-index-get-persisted:getMissSync` | `4300515.598 ops/s` | `4695051.649 ops/s` | `+9.17%` | `better` |
| `segment-index-hot-route-put:putHotRoute` | `3913103.347 ops/s` | `4307964.252 ops/s` | `+10.09%` | `better` |
| `segment-index-hot-route-put:putThenGetHotRoute` | `2027743.124 ops/s` | `2102382.860 ops/s` | `+3.68%` | `better` |
| `segment-index-mixed-drain:partitionedIngestMixed` | `559248.321 ops/s` | `585972.680 ops/s` | `+4.78%` | `better` |
| `segment-index-mixed-drain:partitionedIngestMixed:getWorkload` | `378200.966 ops/s` | `418457.237 ops/s` | `+10.64%` | `better` |
| `segment-index-mixed-drain:partitionedIngestMixed:putWorkload` | `181047.356 ops/s` | `167515.443 ops/s` | `-7.47%` | `worse` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed` | `783679.960 ops/s` | `881011.613 ops/s` | `+12.42%` | `better` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:getWorkload` | `764857.253 ops/s` | `862243.215 ops/s` | `+12.73%` | `better` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:putWorkload` | `18822.706 ops/s` | `18768.398 ops/s` | `-0.29%` | `neutral` |
| `segment-index-persisted-mutation-concurrent:deleteSync` | `7762.050 ops/s` | `6255.636 ops/s` | `-19.41%` | `worse` |
| `segment-index-persisted-mutation-concurrent:putSync` | `7624.602 ops/s` | `6438.771 ops/s` | `-15.55%` | `worse` |
| `segment-index-persisted-mutation:deleteSync` | `3573.011 ops/s` | `2576.206 ops/s` | `-27.90%` | `worse` |
| `segment-index-persisted-mutation:putSync` | `3552.623 ops/s` | `2547.151 ops/s` | `-28.30%` | `worse` |
| `segment-index-range-scan:boundedScan` | `34.903 us/op` | `29.877 us/op` | `-14.40%` | `worse` |
| `segment-index-range-scan:fullStreamRangeFallback` | `2141.080 us/op` | `2099.127 us/op` | `-1.96%` | `neutral` |
| `segment-index-range-scan:sequentialRead` | `4329.613 us/op` | `4159.796 us/op` | `-3.92%` | `warning` |
| `segment-merge-sequential:mergeSequential` | `349.043 us/op` | `326.502 us/op` | `-6.46%` | `warning` |
