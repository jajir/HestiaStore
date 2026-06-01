# Benchmark Comparison

- Profile: `segment-index-pr-smoke`
- Baseline SHA: `afc62792cea444b4d3f438b5c4f0ffbce30e8371`
- Candidate SHA: `d5ffeeaaccc81e4004484e8650d5b9d7fd25e529`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `segment-index-get-live:getHitSync` | `2799725.965 ops/s` | `2898706.610 ops/s` | `+3.54%` | `better` |
| `segment-index-get-live:getMissSync` | `2849105.688 ops/s` | `2546882.412 ops/s` | `-10.61%` | `worse` |
| `segment-index-get-persisted:getHitSync` | `2274327.123 ops/s` | `2608751.236 ops/s` | `+14.70%` | `better` |
| `segment-index-get-persisted:getMissSync` | `2800262.741 ops/s` | `2621404.759 ops/s` | `-6.39%` | `warning` |
| `segment-index-hot-route-put:putHotRoute` | `2696533.982 ops/s` | `2592145.926 ops/s` | `-3.87%` | `warning` |
| `segment-index-hot-route-put:putThenGetHotRoute` | `1455156.007 ops/s` | `1452030.753 ops/s` | `-0.21%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed` | `349927.246 ops/s` | `342895.925 ops/s` | `-2.01%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed:getWorkload` | `139625.304 ops/s` | `119649.930 ops/s` | `-14.31%` | `worse` |
| `segment-index-mixed-drain:partitionedIngestMixed:putWorkload` | `210301.941 ops/s` | `223245.995 ops/s` | `+6.15%` | `better` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed` | `242321.602 ops/s` | `263257.679 ops/s` | `+8.64%` | `better` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:getWorkload` | `223439.127 ops/s` | `245499.645 ops/s` | `+9.87%` | `better` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:putWorkload` | `18882.475 ops/s` | `17758.034 ops/s` | `-5.95%` | `warning` |
