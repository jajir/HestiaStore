# Benchmark Comparison

- Profile: `segment-index-pr-smoke`
- Baseline SHA: `6526d87259948612cba98e2d76f05308f32cccd6`
- Candidate SHA: `a404ccf36f04bd52dfc730f84d0aa935789e7af3`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `segment-index-get-live:getHitSync` | `2324742.489 ops/s` | `2243590.619 ops/s` | `-3.49%` | `warning` |
| `segment-index-get-live:getMissSync` | `2063974.636 ops/s` | `2115057.249 ops/s` | `+2.47%` | `neutral` |
| `segment-index-get-multisegment-hot:getMissSync` | `1866722.729 ops/s` | `-` | `-` | `removed` |
| `segment-index-get-persisted:getHitSync` | `1945002.191 ops/s` | `1931250.563 ops/s` | `-0.71%` | `neutral` |
| `segment-index-get-persisted:getMissSync` | `1951817.011 ops/s` | `2001127.741 ops/s` | `+2.53%` | `neutral` |
| `segment-index-hot-route-put:putHotRoute` | `1997708.939 ops/s` | `2100734.314 ops/s` | `+5.16%` | `better` |
| `segment-index-hot-route-put:putThenGetHotRoute` | `1075095.048 ops/s` | `1060666.028 ops/s` | `-1.34%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed` | `284108.666 ops/s` | `268790.011 ops/s` | `-5.39%` | `warning` |
| `segment-index-mixed-drain:partitionedIngestMixed:getWorkload` | `122494.401 ops/s` | `119081.954 ops/s` | `-2.79%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed:putWorkload` | `161614.265 ops/s` | `149708.057 ops/s` | `-7.37%` | `worse` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed` | `171530.226 ops/s` | `188996.250 ops/s` | `+10.18%` | `better` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:getWorkload` | `158078.022 ops/s` | `173360.200 ops/s` | `+9.67%` | `better` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:putWorkload` | `13452.204 ops/s` | `15636.049 ops/s` | `+16.23%` | `better` |
