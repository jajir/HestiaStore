# Benchmark Comparison

- Profile: `segment-index-pr-smoke`
- Baseline SHA: `d3ef04d30386ff556237923d24a98745e3710b30`
- Candidate SHA: `73478adc47bb23402c32dbba8e034b9f574cc67b`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `segment-index-get-live:getHitSync` | `4707024.508 ops/s` | `5181562.953 ops/s` | `+10.08%` | `better` |
| `segment-index-get-live:getMissSync` | `4495963.676 ops/s` | `4538058.567 ops/s` | `+0.94%` | `neutral` |
| `segment-index-get-multisegment-hot:getHitSync` | `3505546.177 ops/s` | `3579941.298 ops/s` | `+2.12%` | `neutral` |
| `segment-index-get-multisegment-hot:getMissSync` | `4467706.587 ops/s` | `4676471.457 ops/s` | `+4.67%` | `better` |
| `segment-index-get-persisted:getHitSync` | `3216912.159 ops/s` | `3408104.754 ops/s` | `+5.94%` | `better` |
| `segment-index-get-persisted:getMissSync` | `4300515.598 ops/s` | `4854939.932 ops/s` | `+12.89%` | `better` |
| `segment-index-hot-route-put:putHotRoute` | `3913103.347 ops/s` | `4389156.867 ops/s` | `+12.17%` | `better` |
| `segment-index-hot-route-put:putThenGetHotRoute` | `2027743.124 ops/s` | `2247839.842 ops/s` | `+10.85%` | `better` |
| `segment-index-mixed-drain:partitionedIngestMixed` | `559248.321 ops/s` | `524766.565 ops/s` | `-6.17%` | `warning` |
| `segment-index-mixed-drain:partitionedIngestMixed:getWorkload` | `378200.966 ops/s` | `309915.845 ops/s` | `-18.06%` | `worse` |
| `segment-index-mixed-drain:partitionedIngestMixed:putWorkload` | `181047.356 ops/s` | `214850.720 ops/s` | `+18.67%` | `better` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed` | `783679.960 ops/s` | `765246.344 ops/s` | `-2.35%` | `neutral` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:getWorkload` | `764857.253 ops/s` | `734825.867 ops/s` | `-3.93%` | `warning` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:putWorkload` | `18822.706 ops/s` | `30420.477 ops/s` | `+61.62%` | `better` |
| `segment-index-persisted-mutation-concurrent:deleteSync` | `7762.050 ops/s` | `7025.902 ops/s` | `-9.48%` | `worse` |
| `segment-index-persisted-mutation-concurrent:putSync` | `7624.602 ops/s` | `6932.355 ops/s` | `-9.08%` | `worse` |
| `segment-index-persisted-mutation:deleteSync` | `3573.011 ops/s` | `2928.251 ops/s` | `-18.05%` | `worse` |
| `segment-index-persisted-mutation:putSync` | `3552.623 ops/s` | `2889.469 ops/s` | `-18.67%` | `worse` |
| `segment-index-range-scan:boundedScan` | `34.903 us/op` | `28.760 us/op` | `-17.60%` | `worse` |
| `segment-index-range-scan:fullStreamRangeFallback` | `2141.080 us/op` | `2062.669 us/op` | `-3.66%` | `warning` |
| `segment-index-range-scan:sequentialRead` | `4329.613 us/op` | `4064.679 us/op` | `-6.12%` | `warning` |
| `segment-merge-sequential:mergeSequential` | `349.043 us/op` | `325.295 us/op` | `-6.80%` | `warning` |
