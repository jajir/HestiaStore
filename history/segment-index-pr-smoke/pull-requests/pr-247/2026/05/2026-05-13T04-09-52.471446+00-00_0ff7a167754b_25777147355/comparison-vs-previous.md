# Benchmark Comparison

- Profile: `segment-index-pr-smoke`
- Baseline SHA: `7b4e3e01818862a8152f92756c8f59b19c59d7b3`
- Candidate SHA: `0ff7a167754b1c99a3d4b4955a3d771d55268535`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `segment-index-get-live:getHitSync` | `2375112.751 ops/s` | `2314145.555 ops/s` | `-2.57%` | `neutral` |
| `segment-index-get-live:getMissSync` | `4015316.381 ops/s` | `4161991.280 ops/s` | `+3.65%` | `better` |
| `segment-index-get-multisegment-hot:getHitSync` | `7875.263 ops/s` | `7018.285 ops/s` | `-10.88%` | `worse` |
| `segment-index-get-multisegment-hot:getMissSync` | `3696888.041 ops/s` | `3644943.622 ops/s` | `-1.41%` | `neutral` |
| `segment-index-get-persisted:getHitSync` | `127857.612 ops/s` | `2233971.236 ops/s` | `+1647.23%` | `better` |
| `segment-index-get-persisted:getMissSync` | `3997306.616 ops/s` | `3738049.412 ops/s` | `-6.49%` | `warning` |
| `segment-index-hot-route-put:putHotRoute` | `1927965.363 ops/s` | `1994765.495 ops/s` | `+3.46%` | `better` |
| `segment-index-hot-route-put:putThenGetHotRoute` | `1080583.892 ops/s` | `1105694.827 ops/s` | `+2.32%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed` | `293932.124 ops/s` | `282438.309 ops/s` | `-3.91%` | `warning` |
| `segment-index-mixed-drain:partitionedIngestMixed:getWorkload` | `141186.800 ops/s` | `129396.158 ops/s` | `-8.35%` | `worse` |
| `segment-index-mixed-drain:partitionedIngestMixed:putWorkload` | `152745.324 ops/s` | `153042.151 ops/s` | `+0.19%` | `neutral` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed` | `38024.836 ops/s` | `42937.419 ops/s` | `+12.92%` | `better` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:getWorkload` | `32748.187 ops/s` | `37701.416 ops/s` | `+15.13%` | `better` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:putWorkload` | `5276.648 ops/s` | `5236.004 ops/s` | `-0.77%` | `neutral` |
