# Benchmark Comparison

- Profile: `segment-index-pr-smoke`
- Baseline SHA: `c81c3ec81b7222b43972a0eee507b77f33f3672a`
- Candidate SHA: `1a6bf579500a142c3427c80510c8c48230fa9e25`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `segment-index-get-live:getHitSync` | `2239008.192 ops/s` | `2380874.275 ops/s` | `+6.34%` | `better` |
| `segment-index-get-live:getMissSync` | `2266034.484 ops/s` | `2060220.136 ops/s` | `-9.08%` | `worse` |
| `segment-index-get-persisted:getHitSync` | `2162263.124 ops/s` | `1971919.536 ops/s` | `-8.80%` | `worse` |
| `segment-index-get-persisted:getMissSync` | `2184364.254 ops/s` | `2313805.415 ops/s` | `+5.93%` | `better` |
| `segment-index-hot-route-put:putHotRoute` | `2175752.403 ops/s` | `2113794.507 ops/s` | `-2.85%` | `neutral` |
| `segment-index-hot-route-put:putThenGetHotRoute` | `1095516.967 ops/s` | `1131386.238 ops/s` | `+3.27%` | `better` |
| `segment-index-mixed-drain:partitionedIngestMixed` | `467646.275 ops/s` | `409375.536 ops/s` | `-12.46%` | `worse` |
| `segment-index-mixed-drain:partitionedIngestMixed:getWorkload` | `298986.193 ops/s` | `242174.850 ops/s` | `-19.00%` | `worse` |
| `segment-index-mixed-drain:partitionedIngestMixed:putWorkload` | `168660.082 ops/s` | `167200.686 ops/s` | `-0.87%` | `neutral` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed` | `561222.481 ops/s` | `648999.001 ops/s` | `+15.64%` | `better` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:getWorkload` | `547552.251 ops/s` | `633306.615 ops/s` | `+15.66%` | `better` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:putWorkload` | `13670.231 ops/s` | `15692.386 ops/s` | `+14.79%` | `better` |
