# Benchmark Comparison

- Profile: `segment-index-pr-smoke`
- Baseline SHA: `67d6f015595d472d71dbaf369987d5f6eb97b5b5`
- Candidate SHA: `b24c91b4e38a20c9e8206430d6aff5f87188acc6`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `segment-index-get-live:getHitSync` | `2434625.181 ops/s` | `2257560.467 ops/s` | `-7.27%` | `worse` |
| `segment-index-get-live:getMissSync` | `4235514.095 ops/s` | `3517156.013 ops/s` | `-16.96%` | `worse` |
| `segment-index-get-multisegment-hot:getHitSync` | `7489.633 ops/s` | `7458.150 ops/s` | `-0.42%` | `neutral` |
| `segment-index-get-multisegment-hot:getMissSync` | `3814528.118 ops/s` | `3641190.288 ops/s` | `-4.54%` | `warning` |
| `segment-index-get-persisted:getHitSync` | `119572.179 ops/s` | `115767.007 ops/s` | `-3.18%` | `warning` |
| `segment-index-get-persisted:getMissSync` | `3751402.345 ops/s` | `3571036.439 ops/s` | `-4.81%` | `warning` |
| `segment-index-hot-route-put:putHotRoute` | `2010777.929 ops/s` | `2061353.204 ops/s` | `+2.52%` | `neutral` |
| `segment-index-hot-route-put:putThenGetHotRoute` | `1033955.055 ops/s` | `1013535.364 ops/s` | `-1.97%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed` | `283007.487 ops/s` | `322567.514 ops/s` | `+13.98%` | `better` |
| `segment-index-mixed-drain:partitionedIngestMixed:getWorkload` | `137276.471 ops/s` | `142406.493 ops/s` | `+3.74%` | `better` |
| `segment-index-mixed-drain:partitionedIngestMixed:putWorkload` | `145731.016 ops/s` | `180161.021 ops/s` | `+23.63%` | `better` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed` | `41280.902 ops/s` | `46522.515 ops/s` | `+12.70%` | `better` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:getWorkload` | `36114.855 ops/s` | `41164.759 ops/s` | `+13.98%` | `better` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:putWorkload` | `5166.048 ops/s` | `5357.756 ops/s` | `+3.71%` | `better` |
| `segment-index-persisted-mutation:deleteSync` | `2459.668 ops/s` | `3491.136 ops/s` | `+41.94%` | `better` |
| `segment-index-persisted-mutation:putSync` | `440.510 ops/s` | `460.857 ops/s` | `+4.62%` | `better` |
