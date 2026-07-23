# Benchmark Comparison

- Profile: `segment-index-pr-smoke`
- Baseline SHA: `a63e8857313f97e6163f9e4567b7002fa0a469ea`
- Candidate SHA: `96593cd49b374380016362f1fc4d5bacdf709832`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `segment-index-get-live:getHitSync` | `2278134.593 ops/s` | `2276590.346 ops/s` | `-0.07%` | `neutral` |
| `segment-index-get-live:getMissSync` | `2100880.105 ops/s` | `2115488.439 ops/s` | `+0.70%` | `neutral` |
| `segment-index-get-persisted:getHitSync` | `2045610.770 ops/s` | `1946809.455 ops/s` | `-4.83%` | `warning` |
| `segment-index-get-persisted:getMissSync` | `2228607.464 ops/s` | `1949299.575 ops/s` | `-12.53%` | `worse` |
| `segment-index-hot-route-put:putHotRoute` | `2133300.277 ops/s` | `2202417.067 ops/s` | `+3.24%` | `better` |
| `segment-index-hot-route-put:putThenGetHotRoute` | `1112403.013 ops/s` | `1089210.603 ops/s` | `-2.08%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed` | `428311.015 ops/s` | `467952.388 ops/s` | `+9.26%` | `better` |
| `segment-index-mixed-drain:partitionedIngestMixed:getWorkload` | `262493.342 ops/s` | `301146.880 ops/s` | `+14.73%` | `better` |
| `segment-index-mixed-drain:partitionedIngestMixed:putWorkload` | `165817.674 ops/s` | `166805.508 ops/s` | `+0.60%` | `neutral` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed` | `492444.116 ops/s` | `431438.980 ops/s` | `-12.39%` | `worse` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:getWorkload` | `478602.412 ops/s` | `418750.309 ops/s` | `-12.51%` | `worse` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:putWorkload` | `13841.704 ops/s` | `12688.671 ops/s` | `-8.33%` | `worse` |
