# Benchmark Comparison

- Profile: `segment-index-pr-smoke`
- Baseline SHA: `825d249ff4a901a6460ef32f4a00074eb47d28af`
- Candidate SHA: `2b79ad57cfd3465a2b7d0f2a4d77419652aef2d5`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `segment-index-get-live:getHitSync` | `2734197.086 ops/s` | `3084160.965 ops/s` | `+12.80%` | `better` |
| `segment-index-get-live:getMissSync` | `2652042.592 ops/s` | `2616627.254 ops/s` | `-1.34%` | `neutral` |
| `segment-index-get-persisted:getHitSync` | `2429416.833 ops/s` | `2594812.060 ops/s` | `+6.81%` | `better` |
| `segment-index-get-persisted:getMissSync` | `2790975.000 ops/s` | `2594526.968 ops/s` | `-7.04%` | `worse` |
| `segment-index-hot-route-put:putHotRoute` | `2775130.268 ops/s` | `2799675.456 ops/s` | `+0.88%` | `neutral` |
| `segment-index-hot-route-put:putThenGetHotRoute` | `1428354.104 ops/s` | `1411356.056 ops/s` | `-1.19%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed` | `351264.636 ops/s` | `555607.418 ops/s` | `+58.17%` | `better` |
| `segment-index-mixed-drain:partitionedIngestMixed:getWorkload` | `147206.258 ops/s` | `340896.830 ops/s` | `+131.58%` | `better` |
| `segment-index-mixed-drain:partitionedIngestMixed:putWorkload` | `204058.378 ops/s` | `214710.587 ops/s` | `+5.22%` | `better` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed` | `274018.957 ops/s` | `944849.758 ops/s` | `+244.81%` | `better` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:getWorkload` | `256168.550 ops/s` | `926354.904 ops/s` | `+261.62%` | `better` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:putWorkload` | `17850.407 ops/s` | `18494.854 ops/s` | `+3.61%` | `better` |
