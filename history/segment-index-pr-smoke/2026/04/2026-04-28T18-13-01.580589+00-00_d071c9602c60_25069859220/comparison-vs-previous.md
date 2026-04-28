# Benchmark Comparison

- Profile: `segment-index-pr-smoke`
- Baseline SHA: `949f38f2ded71d8f5bcbeaa7d1720096f4c70916`
- Candidate SHA: `d071c9602c6022fca98dfbd1e869ca129e4cd557`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `segment-index-get-live:getHitSync` | `2422002.576 ops/s` | `2146251.189 ops/s` | `-11.39%` | `worse` |
| `segment-index-get-live:getMissSync` | `4111274.976 ops/s` | `3623888.505 ops/s` | `-11.85%` | `worse` |
| `segment-index-get-multisegment-hot:getHitSync` | `7162.551 ops/s` | `7590.125 ops/s` | `+5.97%` | `better` |
| `segment-index-get-multisegment-hot:getMissSync` | `4125678.244 ops/s` | `3557139.759 ops/s` | `-13.78%` | `worse` |
| `segment-index-get-persisted:getHitSync` | `127424.047 ops/s` | `121505.264 ops/s` | `-4.64%` | `warning` |
| `segment-index-get-persisted:getMissSync` | `3875520.778 ops/s` | `3532808.625 ops/s` | `-8.84%` | `worse` |
| `segment-index-hot-route-put:putHotRoute` | `2020650.867 ops/s` | `1965836.839 ops/s` | `-2.71%` | `neutral` |
| `segment-index-hot-route-put:putThenGetHotRoute` | `1084308.901 ops/s` | `1089192.007 ops/s` | `+0.45%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed` | `299998.397 ops/s` | `285913.041 ops/s` | `-4.70%` | `warning` |
| `segment-index-mixed-drain:partitionedIngestMixed:getWorkload` | `125978.371 ops/s` | `107818.573 ops/s` | `-14.42%` | `worse` |
| `segment-index-mixed-drain:partitionedIngestMixed:putWorkload` | `174020.026 ops/s` | `178094.468 ops/s` | `+2.34%` | `neutral` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed` | `44841.564 ops/s` | `49402.235 ops/s` | `+10.17%` | `better` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:getWorkload` | `39483.734 ops/s` | `44096.518 ops/s` | `+11.68%` | `better` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:putWorkload` | `5357.830 ops/s` | `5305.717 ops/s` | `-0.97%` | `neutral` |
| `segment-index-persisted-mutation:deleteSync` | `2801.794 ops/s` | `3460.051 ops/s` | `+23.49%` | `better` |
| `segment-index-persisted-mutation:putSync` | `452.634 ops/s` | `457.164 ops/s` | `+1.00%` | `neutral` |
