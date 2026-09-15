# Benchmark Comparison

- Profile: `segment-index-pr-smoke`
- Baseline SHA: `d3ef04d30386ff556237923d24a98745e3710b30`
- Candidate SHA: `8277d03e70b371e95be39fbbefbb5bb76166df47`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `segment-index-get-live:getHitSync` | `4707024.508 ops/s` | `4814275.096 ops/s` | `+2.28%` | `neutral` |
| `segment-index-get-live:getMissSync` | `4495963.676 ops/s` | `4782117.391 ops/s` | `+6.36%` | `better` |
| `segment-index-get-multisegment-hot:getHitSync` | `3505546.177 ops/s` | `3529098.535 ops/s` | `+0.67%` | `neutral` |
| `segment-index-get-multisegment-hot:getMissSync` | `4467706.587 ops/s` | `4673056.477 ops/s` | `+4.60%` | `better` |
| `segment-index-get-persisted:getHitSync` | `3216912.159 ops/s` | `3345014.068 ops/s` | `+3.98%` | `better` |
| `segment-index-get-persisted:getMissSync` | `4300515.598 ops/s` | `4708918.538 ops/s` | `+9.50%` | `better` |
| `segment-index-hot-route-put:putHotRoute` | `3913103.347 ops/s` | `4313320.417 ops/s` | `+10.23%` | `better` |
| `segment-index-hot-route-put:putThenGetHotRoute` | `2027743.124 ops/s` | `2166978.071 ops/s` | `+6.87%` | `better` |
| `segment-index-mixed-drain:partitionedIngestMixed` | `559248.321 ops/s` | `505070.731 ops/s` | `-9.69%` | `worse` |
| `segment-index-mixed-drain:partitionedIngestMixed:getWorkload` | `378200.966 ops/s` | `335577.209 ops/s` | `-11.27%` | `worse` |
| `segment-index-mixed-drain:partitionedIngestMixed:putWorkload` | `181047.356 ops/s` | `169493.522 ops/s` | `-6.38%` | `warning` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed` | `783679.960 ops/s` | `579793.522 ops/s` | `-26.02%` | `worse` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:getWorkload` | `764857.253 ops/s` | `554997.564 ops/s` | `-27.44%` | `worse` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:putWorkload` | `18822.706 ops/s` | `24795.958 ops/s` | `+31.73%` | `better` |
| `segment-index-persisted-mutation-concurrent:deleteSync` | `7762.050 ops/s` | `6306.229 ops/s` | `-18.76%` | `worse` |
| `segment-index-persisted-mutation-concurrent:putSync` | `7624.602 ops/s` | `6332.896 ops/s` | `-16.94%` | `worse` |
| `segment-index-persisted-mutation:deleteSync` | `3573.011 ops/s` | `2731.759 ops/s` | `-23.54%` | `worse` |
| `segment-index-persisted-mutation:putSync` | `3552.623 ops/s` | `2592.750 ops/s` | `-27.02%` | `worse` |
| `segment-index-range-scan:boundedScan` | `34.903 us/op` | `33.809 us/op` | `-3.13%` | `warning` |
| `segment-index-range-scan:fullStreamRangeFallback` | `2141.080 us/op` | `2141.698 us/op` | `+0.03%` | `neutral` |
| `segment-index-range-scan:sequentialRead` | `4329.613 us/op` | `4016.963 us/op` | `-7.22%` | `worse` |
| `segment-merge-sequential:mergeSequential` | `349.043 us/op` | `325.569 us/op` | `-6.73%` | `warning` |
