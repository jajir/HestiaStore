# Benchmark Comparison

- Profile: `segment-index-pr-smoke`
- Baseline SHA: `de75606802c8f4d476a30a33d4ebe62c4e68eee1`
- Candidate SHA: `f56024616d63c7054f71f78aef833131c8f74d2e`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `segment-index-get-live:getHitSync` | `2177726.945 ops/s` | `2295554.544 ops/s` | `+5.41%` | `better` |
| `segment-index-get-live:getMissSync` | `3614657.693 ops/s` | `3687248.688 ops/s` | `+2.01%` | `neutral` |
| `segment-index-get-multisegment-hot:getHitSync` | `7564.311 ops/s` | `7180.475 ops/s` | `-5.07%` | `warning` |
| `segment-index-get-multisegment-hot:getMissSync` | `3520112.396 ops/s` | `4077203.632 ops/s` | `+15.83%` | `better` |
| `segment-index-get-persisted:getHitSync` | `96817.249 ops/s` | `119479.754 ops/s` | `+23.41%` | `better` |
| `segment-index-get-persisted:getMissSync` | `3478301.561 ops/s` | `3905767.266 ops/s` | `+12.29%` | `better` |
| `segment-index-hot-route-put:putHotRoute` | `1954725.579 ops/s` | `1978302.867 ops/s` | `+1.21%` | `neutral` |
| `segment-index-hot-route-put:putThenGetHotRoute` | `1039414.153 ops/s` | `1077282.961 ops/s` | `+3.64%` | `better` |
| `segment-index-mixed-drain:partitionedIngestMixed` | `294573.866 ops/s` | `298889.977 ops/s` | `+1.47%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed:getWorkload` | `130088.928 ops/s` | `128574.957 ops/s` | `-1.16%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed:putWorkload` | `164484.938 ops/s` | `170315.020 ops/s` | `+3.54%` | `better` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed` | `47561.092 ops/s` | `48494.055 ops/s` | `+1.96%` | `neutral` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:getWorkload` | `42252.221 ops/s` | `43240.398 ops/s` | `+2.34%` | `neutral` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:putWorkload` | `5308.871 ops/s` | `5253.657 ops/s` | `-1.04%` | `neutral` |
| `segment-index-persisted-mutation:deleteSync` | `3512.547 ops/s` | `2861.339 ops/s` | `-18.54%` | `worse` |
| `segment-index-persisted-mutation:putSync` | `454.555 ops/s` | `454.230 ops/s` | `-0.07%` | `neutral` |
