# Benchmark Comparison

- Profile: `segment-index-pr-smoke`
- Baseline SHA: `fa915ff050464f4b44f0424ba5745d7dd4343f06`
- Candidate SHA: `949f38f2ded71d8f5bcbeaa7d1720096f4c70916`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `segment-index-get-live:getHitSync` | `2305079.322 ops/s` | `2422002.576 ops/s` | `+5.07%` | `better` |
| `segment-index-get-live:getMissSync` | `3547611.763 ops/s` | `4111274.976 ops/s` | `+15.89%` | `better` |
| `segment-index-get-multisegment-hot:getHitSync` | `7530.510 ops/s` | `7162.551 ops/s` | `-4.89%` | `warning` |
| `segment-index-get-multisegment-hot:getMissSync` | `3562090.809 ops/s` | `4125678.244 ops/s` | `+15.82%` | `better` |
| `segment-index-get-persisted:getHitSync` | `98445.437 ops/s` | `127424.047 ops/s` | `+29.44%` | `better` |
| `segment-index-get-persisted:getMissSync` | `3566111.797 ops/s` | `3875520.778 ops/s` | `+8.68%` | `better` |
| `segment-index-hot-route-put:putHotRoute` | `1989434.517 ops/s` | `2020650.867 ops/s` | `+1.57%` | `neutral` |
| `segment-index-hot-route-put:putThenGetHotRoute` | `1073847.391 ops/s` | `1084308.901 ops/s` | `+0.97%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed` | `283616.743 ops/s` | `299998.397 ops/s` | `+5.78%` | `better` |
| `segment-index-mixed-drain:partitionedIngestMixed:getWorkload` | `117934.530 ops/s` | `125978.371 ops/s` | `+6.82%` | `better` |
| `segment-index-mixed-drain:partitionedIngestMixed:putWorkload` | `165682.213 ops/s` | `174020.026 ops/s` | `+5.03%` | `better` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed` | `40936.180 ops/s` | `44841.564 ops/s` | `+9.54%` | `better` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:getWorkload` | `35611.415 ops/s` | `39483.734 ops/s` | `+10.87%` | `better` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:putWorkload` | `5324.765 ops/s` | `5357.830 ops/s` | `+0.62%` | `neutral` |
| `segment-index-persisted-mutation:deleteSync` | `3748.212 ops/s` | `2801.794 ops/s` | `-25.25%` | `worse` |
| `segment-index-persisted-mutation:putSync` | `462.161 ops/s` | `452.634 ops/s` | `-2.06%` | `neutral` |
