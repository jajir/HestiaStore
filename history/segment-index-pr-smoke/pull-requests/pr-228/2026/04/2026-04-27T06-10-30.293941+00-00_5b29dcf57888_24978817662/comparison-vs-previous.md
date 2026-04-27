# Benchmark Comparison

- Profile: `segment-index-pr-smoke`
- Baseline SHA: `c51a0e1ce8c5e39b86d5cd734f681c12b84a66c3`
- Candidate SHA: `5b29dcf57888157b6bb0cd2b142cc06433ec9172`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `segment-index-get-live:getHitSync` | `2353308.199 ops/s` | `2346216.029 ops/s` | `-0.30%` | `neutral` |
| `segment-index-get-live:getMissSync` | `3836932.854 ops/s` | `3728426.015 ops/s` | `-2.83%` | `neutral` |
| `segment-index-get-multisegment-hot:getHitSync` | `7598.078 ops/s` | `7590.765 ops/s` | `-0.10%` | `neutral` |
| `segment-index-get-multisegment-hot:getMissSync` | `4119723.069 ops/s` | `3746390.728 ops/s` | `-9.06%` | `worse` |
| `segment-index-get-persisted:getHitSync` | `105790.545 ops/s` | `116074.857 ops/s` | `+9.72%` | `better` |
| `segment-index-get-persisted:getMissSync` | `3757882.481 ops/s` | `3966135.869 ops/s` | `+5.54%` | `better` |
| `segment-index-hot-route-put:putHotRoute` | `1931928.198 ops/s` | `1994992.362 ops/s` | `+3.26%` | `better` |
| `segment-index-hot-route-put:putThenGetHotRoute` | `1112520.890 ops/s` | `1107897.906 ops/s` | `-0.42%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed` | `279991.109 ops/s` | `291631.401 ops/s` | `+4.16%` | `better` |
| `segment-index-mixed-drain:partitionedIngestMixed:getWorkload` | `127502.788 ops/s` | `127168.318 ops/s` | `-0.26%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed:putWorkload` | `152488.321 ops/s` | `164463.083 ops/s` | `+7.85%` | `better` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed` | `41351.234 ops/s` | `40334.998 ops/s` | `-2.46%` | `neutral` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:getWorkload` | `36187.595 ops/s` | `35131.830 ops/s` | `-2.92%` | `neutral` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:putWorkload` | `5163.639 ops/s` | `5203.168 ops/s` | `+0.77%` | `neutral` |
| `segment-index-persisted-mutation:deleteSync` | `2650.029 ops/s` | `2583.838 ops/s` | `-2.50%` | `neutral` |
| `segment-index-persisted-mutation:putSync` | `446.873 ops/s` | `445.989 ops/s` | `-0.20%` | `neutral` |
