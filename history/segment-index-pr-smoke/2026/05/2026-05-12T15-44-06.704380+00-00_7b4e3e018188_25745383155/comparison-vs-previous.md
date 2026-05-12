# Benchmark Comparison

- Profile: `segment-index-pr-smoke`
- Baseline SHA: `f56024616d63c7054f71f78aef833131c8f74d2e`
- Candidate SHA: `7b4e3e01818862a8152f92756c8f59b19c59d7b3`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `segment-index-get-live:getHitSync` | `2295554.544 ops/s` | `2248926.080 ops/s` | `-2.03%` | `neutral` |
| `segment-index-get-live:getMissSync` | `3687248.688 ops/s` | `3641341.838 ops/s` | `-1.25%` | `neutral` |
| `segment-index-get-multisegment-hot:getHitSync` | `7180.475 ops/s` | `7369.662 ops/s` | `+2.63%` | `neutral` |
| `segment-index-get-multisegment-hot:getMissSync` | `4077203.632 ops/s` | `3526630.656 ops/s` | `-13.50%` | `worse` |
| `segment-index-get-persisted:getHitSync` | `119479.754 ops/s` | `108421.256 ops/s` | `-9.26%` | `worse` |
| `segment-index-get-persisted:getMissSync` | `3905767.266 ops/s` | `3402193.480 ops/s` | `-12.89%` | `worse` |
| `segment-index-hot-route-put:putHotRoute` | `1978302.867 ops/s` | `2088238.309 ops/s` | `+5.56%` | `better` |
| `segment-index-hot-route-put:putThenGetHotRoute` | `1077282.961 ops/s` | `1097014.827 ops/s` | `+1.83%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed` | `298889.977 ops/s` | `283194.994 ops/s` | `-5.25%` | `warning` |
| `segment-index-mixed-drain:partitionedIngestMixed:getWorkload` | `128574.957 ops/s` | `115329.569 ops/s` | `-10.30%` | `worse` |
| `segment-index-mixed-drain:partitionedIngestMixed:putWorkload` | `170315.020 ops/s` | `167865.425 ops/s` | `-1.44%` | `neutral` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed` | `48494.055 ops/s` | `49098.075 ops/s` | `+1.25%` | `neutral` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:getWorkload` | `43240.398 ops/s` | `43834.188 ops/s` | `+1.37%` | `neutral` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:putWorkload` | `5253.657 ops/s` | `5263.886 ops/s` | `+0.19%` | `neutral` |
| `segment-index-persisted-mutation:deleteSync` | `2861.339 ops/s` | `-` | `-` | `removed` |
| `segment-index-persisted-mutation:putSync` | `454.230 ops/s` | `-` | `-` | `removed` |
