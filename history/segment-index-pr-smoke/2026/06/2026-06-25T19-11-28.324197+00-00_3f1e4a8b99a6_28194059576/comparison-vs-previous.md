# Benchmark Comparison

- Profile: `segment-index-pr-smoke`
- Baseline SHA: `7c89d40c506b4a34ac676f7212ef7276c51e7308`
- Candidate SHA: `3f1e4a8b99a603b0c0e0972d95816d2e32dbee76`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `segment-index-get-live:getHitSync` | `2189393.102 ops/s` | `2222617.802 ops/s` | `+1.52%` | `neutral` |
| `segment-index-get-live:getMissSync` | `1926612.550 ops/s` | `2058107.459 ops/s` | `+6.83%` | `better` |
| `segment-index-get-persisted:getHitSync` | `1790273.483 ops/s` | `1884081.984 ops/s` | `+5.24%` | `better` |
| `segment-index-get-persisted:getMissSync` | `2106206.484 ops/s` | `2135955.759 ops/s` | `+1.41%` | `neutral` |
| `segment-index-hot-route-put:putHotRoute` | `2108622.875 ops/s` | `2166292.957 ops/s` | `+2.73%` | `neutral` |
| `segment-index-hot-route-put:putThenGetHotRoute` | `1089170.847 ops/s` | `1076339.788 ops/s` | `-1.18%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed` | `318715.813 ops/s` | `310519.694 ops/s` | `-2.57%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed:getWorkload` | `168353.238 ops/s` | `180435.475 ops/s` | `+7.18%` | `better` |
| `segment-index-mixed-drain:partitionedIngestMixed:putWorkload` | `150362.575 ops/s` | `130084.219 ops/s` | `-13.49%` | `worse` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed` | `163384.167 ops/s` | `180755.936 ops/s` | `+10.63%` | `better` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:getWorkload` | `150067.151 ops/s` | `165454.532 ops/s` | `+10.25%` | `better` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:putWorkload` | `13317.016 ops/s` | `15301.404 ops/s` | `+14.90%` | `better` |
