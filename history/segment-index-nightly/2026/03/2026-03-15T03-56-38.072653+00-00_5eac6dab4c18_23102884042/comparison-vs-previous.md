# Benchmark Comparison

- Profile: `segment-index-nightly`
- Baseline SHA: `5eac6dab4c182c00e03ffe09d4c5f1cf167b44f7`
- Candidate SHA: `5eac6dab4c182c00e03ffe09d4c5f1cf167b44f7`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `segment-index-get-overlay:getHitAsyncJoin` | `225133.306 ops/s` | `160120.740 ops/s` | `-28.88%` | `worse` |
| `segment-index-get-overlay:getHitSync` | `3381306.296 ops/s` | `5002418.206 ops/s` | `+47.94%` | `better` |
| `segment-index-get-persisted:getHitAsyncJoin` | `78315.116 ops/s` | `59663.615 ops/s` | `-23.82%` | `worse` |
| `segment-index-get-persisted:getHitSync` | `114266.279 ops/s` | `114170.051 ops/s` | `-0.08%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed` | `534347.335 ops/s` | `564811.072 ops/s` | `+5.70%` | `better` |
| `segment-index-mixed-drain:partitionedIngestMixed:getWorkload` | `528364.232 ops/s` | `558907.592 ops/s` | `+5.78%` | `better` |
| `segment-index-mixed-drain:partitionedIngestMixed:putWorkload` | `5983.103 ops/s` | `5903.480 ops/s` | `-1.33%` | `neutral` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed` | `250705.320 ops/s` | `269649.817 ops/s` | `+7.56%` | `better` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:getWorkload` | `249133.128 ops/s` | `267943.925 ops/s` | `+7.55%` | `better` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:putWorkload` | `1572.192 ops/s` | `1705.892 ops/s` | `+8.50%` | `better` |
