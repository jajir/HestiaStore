# Benchmark Comparison

- Profile: `segment-index-pr-smoke`
- Baseline SHA: `f56024616d63c7054f71f78aef833131c8f74d2e`
- Candidate SHA: `f4ca69adb8a68baf9f663a47abf73bd1311bc669`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `segment-index-get-live:getHitSync` | `2295554.544 ops/s` | `2318517.162 ops/s` | `+1.00%` | `neutral` |
| `segment-index-get-live:getMissSync` | `3687248.688 ops/s` | `3865381.023 ops/s` | `+4.83%` | `better` |
| `segment-index-get-multisegment-hot:getHitSync` | `7180.475 ops/s` | `7399.190 ops/s` | `+3.05%` | `better` |
| `segment-index-get-multisegment-hot:getMissSync` | `4077203.632 ops/s` | `3688019.554 ops/s` | `-9.55%` | `worse` |
| `segment-index-get-persisted:getHitSync` | `119479.754 ops/s` | `124093.873 ops/s` | `+3.86%` | `better` |
| `segment-index-get-persisted:getMissSync` | `3905767.266 ops/s` | `4160984.456 ops/s` | `+6.53%` | `better` |
| `segment-index-hot-route-put:putHotRoute` | `1978302.867 ops/s` | `2059107.341 ops/s` | `+4.08%` | `better` |
| `segment-index-hot-route-put:putThenGetHotRoute` | `1077282.961 ops/s` | `1073198.932 ops/s` | `-0.38%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed` | `298889.977 ops/s` | `304631.811 ops/s` | `+1.92%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed:getWorkload` | `128574.957 ops/s` | `144469.587 ops/s` | `+12.36%` | `better` |
| `segment-index-mixed-drain:partitionedIngestMixed:putWorkload` | `170315.020 ops/s` | `160162.225 ops/s` | `-5.96%` | `warning` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed` | `48494.055 ops/s` | `42362.101 ops/s` | `-12.64%` | `worse` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:getWorkload` | `43240.398 ops/s` | `37142.654 ops/s` | `-14.10%` | `worse` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:putWorkload` | `5253.657 ops/s` | `5219.447 ops/s` | `-0.65%` | `neutral` |
| `segment-index-persisted-mutation:deleteSync` | `2861.339 ops/s` | `-` | `-` | `removed` |
| `segment-index-persisted-mutation:putSync` | `454.230 ops/s` | `-` | `-` | `removed` |
