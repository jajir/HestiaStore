# Benchmark Comparison

- Profile: `segment-index-pr-smoke`
- Baseline SHA: `12b89ba64f4bca39f5521e18ca949e04aa583e4f`
- Candidate SHA: `c1b567dbe18ec335f34b061586c4c20e769967e1`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `segment-index-get-live:getHitSync` | `3355813.707 ops/s` | `3236186.734 ops/s` | `-3.56%` | `warning` |
| `segment-index-get-live:getMissSync` | `3445228.728 ops/s` | `3737726.934 ops/s` | `+8.49%` | `better` |
| `segment-index-get-multisegment-hot:getHitSync` | `84.235 ops/s` | `76.901 ops/s` | `-8.71%` | `worse` |
| `segment-index-get-multisegment-hot:getMissSync` | `3494804.409 ops/s` | `3746441.143 ops/s` | `+7.20%` | `better` |
| `segment-index-get-persisted:getHitSync` | `123780.724 ops/s` | `110358.263 ops/s` | `-10.84%` | `worse` |
| `segment-index-get-persisted:getMissSync` | `3813452.862 ops/s` | `3695320.539 ops/s` | `-3.10%` | `warning` |
| `segment-index-hot-route-put:putHotRoute` | `2782876.871 ops/s` | `2706941.534 ops/s` | `-2.73%` | `neutral` |
| `segment-index-hot-route-put:putThenGetHotRoute` | `1502603.536 ops/s` | `1531347.916 ops/s` | `+1.91%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed` | `355658.360 ops/s` | `346125.979 ops/s` | `-2.68%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed:getWorkload` | `172919.880 ops/s` | `179407.565 ops/s` | `+3.75%` | `better` |
| `segment-index-mixed-drain:partitionedIngestMixed:putWorkload` | `182738.481 ops/s` | `166718.414 ops/s` | `-8.77%` | `worse` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed` | `205024.453 ops/s` | `103422.338 ops/s` | `-49.56%` | `worse` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:getWorkload` | `393.101 ops/s` | `85313.035 ops/s` | `+21602.56%` | `better` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:putWorkload` | `204631.352 ops/s` | `18109.303 ops/s` | `-91.15%` | `worse` |
| `segment-index-persisted-mutation:deleteSync` | `3471.263 ops/s` | `3503.462 ops/s` | `+0.93%` | `neutral` |
| `segment-index-persisted-mutation:putSync` | `3318.869 ops/s` | `3286.092 ops/s` | `-0.99%` | `neutral` |
