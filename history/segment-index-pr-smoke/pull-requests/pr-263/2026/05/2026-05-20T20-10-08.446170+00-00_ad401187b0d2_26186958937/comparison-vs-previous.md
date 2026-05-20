# Benchmark Comparison

- Profile: `segment-index-pr-smoke`
- Baseline SHA: `24a53ff220604e4a2d02e63b54909f2f91ff0ce1`
- Candidate SHA: `ad401187b0d280774b5f199930b57f3f9581b701`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `segment-index-get-live:getHitSync` | `2100032.607 ops/s` | `2181062.271 ops/s` | `+3.86%` | `better` |
| `segment-index-get-live:getMissSync` | `1997472.610 ops/s` | `2155556.313 ops/s` | `+7.91%` | `better` |
| `segment-index-get-persisted:getHitSync` | `1798213.027 ops/s` | `1882057.048 ops/s` | `+4.66%` | `better` |
| `segment-index-get-persisted:getMissSync` | `2147276.469 ops/s` | `2119758.694 ops/s` | `-1.28%` | `neutral` |
| `segment-index-hot-route-put:putHotRoute` | `2099016.377 ops/s` | `2000217.623 ops/s` | `-4.71%` | `warning` |
| `segment-index-hot-route-put:putThenGetHotRoute` | `1127133.202 ops/s` | `1041884.031 ops/s` | `-7.56%` | `worse` |
| `segment-index-mixed-drain:partitionedIngestMixed` | `299823.786 ops/s` | `289971.572 ops/s` | `-3.29%` | `warning` |
| `segment-index-mixed-drain:partitionedIngestMixed:getWorkload` | `120218.277 ops/s` | `125485.344 ops/s` | `+4.38%` | `better` |
| `segment-index-mixed-drain:partitionedIngestMixed:putWorkload` | `179605.509 ops/s` | `164486.229 ops/s` | `-8.42%` | `worse` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed` | `169460.354 ops/s` | `171356.261 ops/s` | `+1.12%` | `neutral` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:getWorkload` | `155309.091 ops/s` | `155838.806 ops/s` | `+0.34%` | `neutral` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:putWorkload` | `14151.263 ops/s` | `15517.455 ops/s` | `+9.65%` | `better` |
