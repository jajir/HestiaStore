# Benchmark Comparison

- Profile: `segment-index-pr-smoke`
- Baseline SHA: `4d7ed0e894ecbc0e65a88805b11ec84b6b680e9c`
- Candidate SHA: `ffda3aaec23c2b7cf53c98b773e52e13993200a5`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `segment-index-get-live:getHitSync` | `2507781.117 ops/s` | `2267663.087 ops/s` | `-9.57%` | `worse` |
| `segment-index-get-live:getMissSync` | `2143137.899 ops/s` | `2197983.231 ops/s` | `+2.56%` | `neutral` |
| `segment-index-get-persisted:getHitSync` | `1837613.395 ops/s` | `1944629.552 ops/s` | `+5.82%` | `better` |
| `segment-index-get-persisted:getMissSync` | `2354231.518 ops/s` | `2120222.296 ops/s` | `-9.94%` | `worse` |
| `segment-index-hot-route-put:putHotRoute` | `2180172.725 ops/s` | `2170394.458 ops/s` | `-0.45%` | `neutral` |
| `segment-index-hot-route-put:putThenGetHotRoute` | `1092164.267 ops/s` | `1073740.038 ops/s` | `-1.69%` | `neutral` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed` | `-` | `378272.309 ops/s` | `-` | `new` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:getWorkload` | `-` | `366499.370 ops/s` | `-` | `new` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:putWorkload` | `-` | `11772.939 ops/s` | `-` | `new` |
