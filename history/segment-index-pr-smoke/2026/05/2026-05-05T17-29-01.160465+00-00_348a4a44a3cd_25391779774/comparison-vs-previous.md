# Benchmark Comparison

- Profile: `segment-index-pr-smoke`
- Baseline SHA: `1bf3c96c4778c8cca6edc361e68f8c45fc3d9083`
- Candidate SHA: `348a4a44a3cd4a5cc0ebc3dabc1e79e31a36628e`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `segment-index-get-live:getHitSync` | `2369528.705 ops/s` | `1874630.676 ops/s` | `-20.89%` | `worse` |
| `segment-index-get-live:getMissSync` | `3730179.464 ops/s` | `2397368.890 ops/s` | `-35.73%` | `worse` |
| `segment-index-get-multisegment-hot:getHitSync` | `7132.769 ops/s` | `11879.180 ops/s` | `+66.54%` | `better` |
| `segment-index-get-multisegment-hot:getMissSync` | `3726597.753 ops/s` | `2333748.602 ops/s` | `-37.38%` | `worse` |
| `segment-index-get-persisted:getHitSync` | `119024.024 ops/s` | `114103.764 ops/s` | `-4.13%` | `warning` |
| `segment-index-get-persisted:getMissSync` | `4003546.285 ops/s` | `2363486.509 ops/s` | `-40.97%` | `worse` |
| `segment-index-hot-route-put:putHotRoute` | `1920380.902 ops/s` | `1888145.379 ops/s` | `-1.68%` | `neutral` |
| `segment-index-hot-route-put:putThenGetHotRoute` | `1087420.729 ops/s` | `989004.445 ops/s` | `-9.05%` | `worse` |
| `segment-index-mixed-drain:partitionedIngestMixed` | `289156.511 ops/s` | `259705.846 ops/s` | `-10.19%` | `worse` |
| `segment-index-mixed-drain:partitionedIngestMixed:getWorkload` | `124584.710 ops/s` | `112452.819 ops/s` | `-9.74%` | `worse` |
| `segment-index-mixed-drain:partitionedIngestMixed:putWorkload` | `164571.801 ops/s` | `147253.027 ops/s` | `-10.52%` | `worse` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed` | `41398.166 ops/s` | `43369.598 ops/s` | `+4.76%` | `better` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:getWorkload` | `36190.796 ops/s` | `38186.644 ops/s` | `+5.51%` | `better` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:putWorkload` | `5207.370 ops/s` | `5182.955 ops/s` | `-0.47%` | `neutral` |
| `segment-index-persisted-mutation:deleteSync` | `2547.386 ops/s` | `2578.500 ops/s` | `+1.22%` | `neutral` |
| `segment-index-persisted-mutation:putSync` | `448.434 ops/s` | `445.070 ops/s` | `-0.75%` | `neutral` |
