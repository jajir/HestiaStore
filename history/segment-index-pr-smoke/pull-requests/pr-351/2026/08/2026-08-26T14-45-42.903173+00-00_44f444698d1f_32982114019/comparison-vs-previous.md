# Benchmark Comparison

- Profile: `segment-index-pr-smoke`
- Baseline SHA: `d3ef04d30386ff556237923d24a98745e3710b30`
- Candidate SHA: `44f444698d1f8e04669e08277f0f2f750e929af3`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `segment-index-get-live:getHitSync` | `4707024.508 ops/s` | `5912807.799 ops/s` | `+25.62%` | `better` |
| `segment-index-get-live:getMissSync` | `4495963.676 ops/s` | `5903081.445 ops/s` | `+31.30%` | `better` |
| `segment-index-get-multisegment-hot:getHitSync` | `3505546.177 ops/s` | `4526153.090 ops/s` | `+29.11%` | `better` |
| `segment-index-get-multisegment-hot:getMissSync` | `4467706.587 ops/s` | `5848477.240 ops/s` | `+30.91%` | `better` |
| `segment-index-get-persisted:getHitSync` | `3216912.159 ops/s` | `4509792.470 ops/s` | `+40.19%` | `better` |
| `segment-index-get-persisted:getMissSync` | `4300515.598 ops/s` | `6315890.391 ops/s` | `+46.86%` | `better` |
| `segment-index-hot-route-put:putHotRoute` | `3913103.347 ops/s` | `5181697.354 ops/s` | `+32.42%` | `better` |
| `segment-index-hot-route-put:putThenGetHotRoute` | `2027743.124 ops/s` | `2780890.792 ops/s` | `+37.14%` | `better` |
| `segment-index-mixed-drain:partitionedIngestMixed` | `559248.321 ops/s` | `667752.168 ops/s` | `+19.40%` | `better` |
| `segment-index-mixed-drain:partitionedIngestMixed:getWorkload` | `378200.966 ops/s` | `422106.781 ops/s` | `+11.61%` | `better` |
| `segment-index-mixed-drain:partitionedIngestMixed:putWorkload` | `181047.356 ops/s` | `245645.387 ops/s` | `+35.68%` | `better` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed` | `783679.960 ops/s` | `932372.715 ops/s` | `+18.97%` | `better` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:getWorkload` | `764857.253 ops/s` | `913174.544 ops/s` | `+19.39%` | `better` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:putWorkload` | `18822.706 ops/s` | `19198.171 ops/s` | `+1.99%` | `neutral` |
| `segment-index-persisted-mutation-concurrent:deleteSync` | `7762.050 ops/s` | `3691.177 ops/s` | `-52.45%` | `worse` |
| `segment-index-persisted-mutation-concurrent:putSync` | `7624.602 ops/s` | `3761.371 ops/s` | `-50.67%` | `worse` |
| `segment-index-persisted-mutation:deleteSync` | `3573.011 ops/s` | `2034.016 ops/s` | `-43.07%` | `worse` |
| `segment-index-persisted-mutation:putSync` | `3552.623 ops/s` | `2188.724 ops/s` | `-38.39%` | `worse` |
| `segment-index-range-scan:boundedScan` | `34.903 us/op` | `22.784 us/op` | `-34.72%` | `worse` |
| `segment-index-range-scan:fullStreamRangeFallback` | `2141.080 us/op` | `1606.708 us/op` | `-24.96%` | `worse` |
| `segment-index-range-scan:sequentialRead` | `4329.613 us/op` | `3088.757 us/op` | `-28.66%` | `worse` |
| `segment-merge-sequential:mergeSequential` | `349.043 us/op` | `273.359 us/op` | `-21.68%` | `worse` |
