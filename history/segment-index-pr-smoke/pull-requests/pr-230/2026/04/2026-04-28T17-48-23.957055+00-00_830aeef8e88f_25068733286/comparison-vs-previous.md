# Benchmark Comparison

- Profile: `segment-index-pr-smoke`
- Baseline SHA: `949f38f2ded71d8f5bcbeaa7d1720096f4c70916`
- Candidate SHA: `830aeef8e88f981f7b1e00c1f8e5a5e8bca11f93`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `segment-index-get-live:getHitSync` | `2422002.576 ops/s` | `2101600.218 ops/s` | `-13.23%` | `worse` |
| `segment-index-get-live:getMissSync` | `4111274.976 ops/s` | `3632648.111 ops/s` | `-11.64%` | `worse` |
| `segment-index-get-multisegment-hot:getHitSync` | `7162.551 ops/s` | `7782.907 ops/s` | `+8.66%` | `better` |
| `segment-index-get-multisegment-hot:getMissSync` | `4125678.244 ops/s` | `3571732.076 ops/s` | `-13.43%` | `worse` |
| `segment-index-get-persisted:getHitSync` | `127424.047 ops/s` | `114955.089 ops/s` | `-9.79%` | `worse` |
| `segment-index-get-persisted:getMissSync` | `3875520.778 ops/s` | `3432974.893 ops/s` | `-11.42%` | `worse` |
| `segment-index-hot-route-put:putHotRoute` | `2020650.867 ops/s` | `2123825.197 ops/s` | `+5.11%` | `better` |
| `segment-index-hot-route-put:putThenGetHotRoute` | `1084308.901 ops/s` | `1106891.112 ops/s` | `+2.08%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed` | `299998.397 ops/s` | `305415.827 ops/s` | `+1.81%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed:getWorkload` | `125978.371 ops/s` | `122375.582 ops/s` | `-2.86%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed:putWorkload` | `174020.026 ops/s` | `183040.244 ops/s` | `+5.18%` | `better` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed` | `44841.564 ops/s` | `42422.639 ops/s` | `-5.39%` | `warning` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:getWorkload` | `39483.734 ops/s` | `37175.427 ops/s` | `-5.85%` | `warning` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:putWorkload` | `5357.830 ops/s` | `5247.212 ops/s` | `-2.06%` | `neutral` |
| `segment-index-persisted-mutation:deleteSync` | `2801.794 ops/s` | `3517.682 ops/s` | `+25.55%` | `better` |
| `segment-index-persisted-mutation:putSync` | `452.634 ops/s` | `462.649 ops/s` | `+2.21%` | `neutral` |
