# Benchmark Comparison

- Profile: `segment-index-nightly`
- Baseline SHA: `52b04525c10e1f18faddd3a3d7f018427f1a5b53`
- Candidate SHA: `1b1356d5b078268099361347568320d48c7849f3`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `segment-index-get-overlay:getHitAsyncJoin` | `165752.729 ops/s` | `157241.536 ops/s` | `-5.13%` | `warning` |
| `segment-index-get-overlay:getHitSync` | `5250245.326 ops/s` | `4903591.401 ops/s` | `-6.60%` | `warning` |
| `segment-index-get-persisted:getHitAsyncJoin` | `60497.436 ops/s` | `60436.137 ops/s` | `-0.10%` | `neutral` |
| `segment-index-get-persisted:getHitSync` | `113666.186 ops/s` | `115340.973 ops/s` | `+1.47%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed` | `533739.220 ops/s` | `536109.366 ops/s` | `+0.44%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed:getWorkload` | `527921.237 ops/s` | `529955.822 ops/s` | `+0.39%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed:putWorkload` | `5817.983 ops/s` | `6153.544 ops/s` | `+5.77%` | `better` |
