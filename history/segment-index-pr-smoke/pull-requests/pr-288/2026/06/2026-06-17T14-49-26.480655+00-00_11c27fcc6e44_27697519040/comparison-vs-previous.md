# Benchmark Comparison

- Profile: `segment-index-pr-smoke`
- Baseline SHA: `6526d87259948612cba98e2d76f05308f32cccd6`
- Candidate SHA: `11c27fcc6e4451d2ffeb8f9e3523c5c13228403a`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `segment-index-get-live:getHitSync` | `2152730.046 ops/s` | `2428702.373 ops/s` | `+12.82%` | `better` |
| `segment-index-get-live:getMissSync` | `2276902.304 ops/s` | `2170292.694 ops/s` | `-4.68%` | `warning` |
| `segment-index-get-persisted:getHitSync` | `1645170.483 ops/s` | `1670608.069 ops/s` | `+1.55%` | `neutral` |
| `segment-index-get-persisted:getMissSync` | `2295128.108 ops/s` | `1993740.094 ops/s` | `-13.13%` | `worse` |
| `segment-index-hot-route-put:putHotRoute` | `2055984.503 ops/s` | `2066355.428 ops/s` | `+0.50%` | `neutral` |
| `segment-index-hot-route-put:putThenGetHotRoute` | `1175656.226 ops/s` | `1161839.292 ops/s` | `-1.18%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed` | `305937.094 ops/s` | `308763.185 ops/s` | `+0.92%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed:getWorkload` | `157705.243 ops/s` | `157926.066 ops/s` | `+0.14%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed:putWorkload` | `148231.851 ops/s` | `150837.119 ops/s` | `+1.76%` | `neutral` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed` | `161613.495 ops/s` | `166856.806 ops/s` | `+3.24%` | `better` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:getWorkload` | `147345.516 ops/s` | `153623.950 ops/s` | `+4.26%` | `better` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:putWorkload` | `14267.979 ops/s` | `13232.856 ops/s` | `-7.25%` | `worse` |
