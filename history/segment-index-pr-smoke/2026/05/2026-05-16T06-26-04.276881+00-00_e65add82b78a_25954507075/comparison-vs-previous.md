# Benchmark Comparison

- Profile: `segment-index-pr-smoke`
- Baseline SHA: `b340118f6122456eaa4ec7c9e81f73ccfd7d8f86`
- Candidate SHA: `e65add82b78aa708740ea09faa5b47e2f6dcb992`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `segment-index-get-live:getHitSync` | `2346385.319 ops/s` | `2487245.340 ops/s` | `+6.00%` | `better` |
| `segment-index-get-live:getMissSync` | `3788572.875 ops/s` | `4029156.703 ops/s` | `+6.35%` | `better` |
| `segment-index-get-multisegment-hot:getHitSync` | `7006.097 ops/s` | `7157.474 ops/s` | `+2.16%` | `neutral` |
| `segment-index-get-multisegment-hot:getMissSync` | `3658884.662 ops/s` | `3577183.152 ops/s` | `-2.23%` | `neutral` |
| `segment-index-get-persisted:getHitSync` | `1874581.663 ops/s` | `1916848.221 ops/s` | `+2.25%` | `neutral` |
| `segment-index-get-persisted:getMissSync` | `3752926.287 ops/s` | `3895187.297 ops/s` | `+3.79%` | `better` |
| `segment-index-hot-route-put:putHotRoute` | `1894169.785 ops/s` | `1830052.053 ops/s` | `-3.39%` | `warning` |
| `segment-index-hot-route-put:putThenGetHotRoute` | `1085972.026 ops/s` | `1032732.771 ops/s` | `-4.90%` | `warning` |
| `segment-index-mixed-drain:partitionedIngestMixed` | `294196.171 ops/s` | `280549.239 ops/s` | `-4.64%` | `warning` |
| `segment-index-mixed-drain:partitionedIngestMixed:getWorkload` | `125864.808 ops/s` | `120556.495 ops/s` | `-4.22%` | `warning` |
| `segment-index-mixed-drain:partitionedIngestMixed:putWorkload` | `168331.363 ops/s` | `159992.744 ops/s` | `-4.95%` | `warning` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed` | `43802.390 ops/s` | `38398.955 ops/s` | `-12.34%` | `worse` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:getWorkload` | `38485.145 ops/s` | `33165.711 ops/s` | `-13.82%` | `worse` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:putWorkload` | `5317.245 ops/s` | `5233.244 ops/s` | `-1.58%` | `neutral` |
