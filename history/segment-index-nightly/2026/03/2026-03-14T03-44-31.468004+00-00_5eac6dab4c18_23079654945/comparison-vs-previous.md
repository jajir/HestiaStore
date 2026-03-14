# Benchmark Comparison

- Profile: `segment-index-nightly`
- Baseline SHA: `5eac6dab4c182c00e03ffe09d4c5f1cf167b44f7`
- Candidate SHA: `5eac6dab4c182c00e03ffe09d4c5f1cf167b44f7`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `segment-index-get-overlay:getHitAsyncJoin` | `163727.800 ops/s` | `225133.306 ops/s` | `+37.50%` | `better` |
| `segment-index-get-overlay:getHitSync` | `5328126.976 ops/s` | `3381306.296 ops/s` | `-36.54%` | `worse` |
| `segment-index-get-persisted:getHitAsyncJoin` | `59709.223 ops/s` | `78315.116 ops/s` | `+31.16%` | `better` |
| `segment-index-get-persisted:getHitSync` | `113462.829 ops/s` | `114266.279 ops/s` | `+0.71%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed` | `495116.290 ops/s` | `534347.335 ops/s` | `+7.92%` | `better` |
| `segment-index-mixed-drain:partitionedIngestMixed:getWorkload` | `489270.495 ops/s` | `528364.232 ops/s` | `+7.99%` | `better` |
| `segment-index-mixed-drain:partitionedIngestMixed:putWorkload` | `5845.796 ops/s` | `5983.103 ops/s` | `+2.35%` | `neutral` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed` | `-` | `250705.320 ops/s` | `-` | `new` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:getWorkload` | `-` | `249133.128 ops/s` | `-` | `new` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:putWorkload` | `-` | `1572.192 ops/s` | `-` | `new` |
