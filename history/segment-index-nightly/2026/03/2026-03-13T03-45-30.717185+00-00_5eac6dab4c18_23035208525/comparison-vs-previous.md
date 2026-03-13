# Benchmark Comparison

- Profile: `segment-index-nightly`
- Baseline SHA: `1b1356d5b078268099361347568320d48c7849f3`
- Candidate SHA: `5eac6dab4c182c00e03ffe09d4c5f1cf167b44f7`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `segment-index-get-overlay:getHitAsyncJoin` | `157241.536 ops/s` | `163727.800 ops/s` | `+4.13%` | `better` |
| `segment-index-get-overlay:getHitSync` | `4903591.401 ops/s` | `5328126.976 ops/s` | `+8.66%` | `better` |
| `segment-index-get-persisted:getHitAsyncJoin` | `60436.137 ops/s` | `59709.223 ops/s` | `-1.20%` | `neutral` |
| `segment-index-get-persisted:getHitSync` | `115340.973 ops/s` | `113462.829 ops/s` | `-1.63%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed` | `536109.366 ops/s` | `495116.290 ops/s` | `-7.65%` | `worse` |
| `segment-index-mixed-drain:partitionedIngestMixed:getWorkload` | `529955.822 ops/s` | `489270.495 ops/s` | `-7.68%` | `worse` |
| `segment-index-mixed-drain:partitionedIngestMixed:putWorkload` | `6153.544 ops/s` | `5845.796 ops/s` | `-5.00%` | `warning` |
