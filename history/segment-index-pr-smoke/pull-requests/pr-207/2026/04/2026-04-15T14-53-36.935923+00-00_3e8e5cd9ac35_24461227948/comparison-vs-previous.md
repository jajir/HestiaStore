# Benchmark Comparison

- Profile: `segment-index-pr-smoke`
- Baseline SHA: `30f93a9be86df7361bbeb5cf93c3547cbb155d00`
- Candidate SHA: `3e8e5cd9ac35f95778711b1ded406339fc4493b1`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `segment-index-get-live:getHitSync` | `3575180.962 ops/s` | `3706040.004 ops/s` | `+3.66%` | `better` |
| `segment-index-get-live:getMissSync` | `3895545.853 ops/s` | `4214965.167 ops/s` | `+8.20%` | `better` |
| `segment-index-get-multisegment-hot:getHitSync` | `82.338 ops/s` | `214.930 ops/s` | `+161.03%` | `better` |
| `segment-index-get-multisegment-hot:getMissSync` | `4009765.373 ops/s` | `3961128.171 ops/s` | `-1.21%` | `neutral` |
| `segment-index-get-persisted:getHitSync` | `124782.317 ops/s` | `107135.287 ops/s` | `-14.14%` | `worse` |
| `segment-index-get-persisted:getMissSync` | `4146936.470 ops/s` | `4350856.611 ops/s` | `+4.92%` | `better` |
| `segment-index-hot-route-put:putHotRoute` | `2405670.264 ops/s` | `2344145.558 ops/s` | `-2.56%` | `neutral` |
| `segment-index-hot-route-put:putThenGetHotRoute` | `1506257.259 ops/s` | `1365944.319 ops/s` | `-9.32%` | `worse` |
| `segment-index-mixed-drain:partitionedIngestMixed` | `363066.668 ops/s` | `376093.521 ops/s` | `+3.59%` | `better` |
| `segment-index-mixed-drain:partitionedIngestMixed:getWorkload` | `190267.107 ops/s` | `204935.497 ops/s` | `+7.71%` | `better` |
| `segment-index-mixed-drain:partitionedIngestMixed:putWorkload` | `172799.561 ops/s` | `171158.024 ops/s` | `-0.95%` | `neutral` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed` | `45755.189 ops/s` | `35595.561 ops/s` | `-22.20%` | `worse` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:getWorkload` | `39243.098 ops/s` | `26654.180 ops/s` | `-32.08%` | `worse` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:putWorkload` | `6512.091 ops/s` | `8941.381 ops/s` | `+37.30%` | `better` |
| `segment-index-persisted-mutation:deleteSync` | `2590.389 ops/s` | `2689.831 ops/s` | `+3.84%` | `better` |
| `segment-index-persisted-mutation:putSync` | `2494.593 ops/s` | `2557.460 ops/s` | `+2.52%` | `neutral` |
