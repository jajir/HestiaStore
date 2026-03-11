# Benchmark Comparison

- Profile: `segment-index-nightly`
- Baseline SHA: `52b04525c10e1f18faddd3a3d7f018427f1a5b53`
- Candidate SHA: `52b04525c10e1f18faddd3a3d7f018427f1a5b53`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `segment-index-get-overlay:getHitAsyncJoin` | `165534.327 ops/s` | `165752.729 ops/s` | `+0.13%` | `neutral` |
| `segment-index-get-overlay:getHitSync` | `5126460.225 ops/s` | `5250245.326 ops/s` | `+2.41%` | `neutral` |
| `segment-index-get-persisted:getHitAsyncJoin` | `59797.494 ops/s` | `60497.436 ops/s` | `+1.17%` | `neutral` |
| `segment-index-get-persisted:getHitSync` | `113314.902 ops/s` | `113666.186 ops/s` | `+0.31%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed` | `527180.221 ops/s` | `533739.220 ops/s` | `+1.24%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed:getWorkload` | `521201.090 ops/s` | `527921.237 ops/s` | `+1.29%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed:putWorkload` | `5979.131 ops/s` | `5817.983 ops/s` | `-2.70%` | `neutral` |
