# Benchmark Comparison

- Profile: `segment-index-pr-smoke`
- Baseline SHA: `3af5226638894ad29c7a620a80e11f651393a855`
- Candidate SHA: `38bb61d43b1d0060915e72de6cd95be2144ac9b8`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `segment-index-get-live:getHitSync` | `2356417.476 ops/s` | `2265021.311 ops/s` | `-3.88%` | `warning` |
| `segment-index-get-live:getMissSync` | `2223573.626 ops/s` | `2117807.358 ops/s` | `-4.76%` | `warning` |
| `segment-index-get-persisted:getHitSync` | `2039659.994 ops/s` | `1657111.114 ops/s` | `-18.76%` | `worse` |
| `segment-index-get-persisted:getMissSync` | `2248412.380 ops/s` | `2397496.425 ops/s` | `+6.63%` | `better` |
| `segment-index-hot-route-put:putHotRoute` | `2207315.807 ops/s` | `2146022.694 ops/s` | `-2.78%` | `neutral` |
| `segment-index-hot-route-put:putThenGetHotRoute` | `1076039.675 ops/s` | `1113505.403 ops/s` | `+3.48%` | `better` |
| `segment-index-mixed-drain:partitionedIngestMixed` | `418136.769 ops/s` | `485528.882 ops/s` | `+16.12%` | `better` |
| `segment-index-mixed-drain:partitionedIngestMixed:getWorkload` | `252636.638 ops/s` | `326115.448 ops/s` | `+29.08%` | `better` |
| `segment-index-mixed-drain:partitionedIngestMixed:putWorkload` | `165500.131 ops/s` | `159413.434 ops/s` | `-3.68%` | `warning` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed` | `512866.417 ops/s` | `633302.077 ops/s` | `+23.48%` | `better` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:getWorkload` | `499073.727 ops/s` | `618605.474 ops/s` | `+23.95%` | `better` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:putWorkload` | `13792.690 ops/s` | `14696.603 ops/s` | `+6.55%` | `better` |
