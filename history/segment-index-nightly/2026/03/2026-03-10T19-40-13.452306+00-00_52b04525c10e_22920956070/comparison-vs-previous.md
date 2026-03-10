# Benchmark Comparison

- Bootstrap mode: no prior canonical baseline exists yet, and fallback git baseline predates the benchmark harness.
- Candidate results below are the first canonical benchmark snapshot for this workflow.


- Profile: `segment-index-nightly`
- Baseline SHA: `52b04525c10e1f18faddd3a3d7f018427f1a5b53`
- Candidate SHA: `52b04525c10e1f18faddd3a3d7f018427f1a5b53`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `segment-index-get-overlay:getHitAsyncJoin` | `165534.327 ops/s` | `165534.327 ops/s` | `+0.00%` | `neutral` |
| `segment-index-get-overlay:getHitSync` | `5126460.225 ops/s` | `5126460.225 ops/s` | `+0.00%` | `neutral` |
| `segment-index-get-persisted:getHitAsyncJoin` | `59797.494 ops/s` | `59797.494 ops/s` | `+0.00%` | `neutral` |
| `segment-index-get-persisted:getHitSync` | `113314.902 ops/s` | `113314.902 ops/s` | `+0.00%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed` | `527180.221 ops/s` | `527180.221 ops/s` | `+0.00%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed:getWorkload` | `521201.090 ops/s` | `521201.090 ops/s` | `+0.00%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed:putWorkload` | `5979.131 ops/s` | `5979.131 ops/s` | `+0.00%` | `neutral` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed` | `273741.952 ops/s` | `273741.952 ops/s` | `+0.00%` | `neutral` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:getWorkload` | `272255.711 ops/s` | `272255.711 ops/s` | `+0.00%` | `neutral` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:putWorkload` | `1486.241 ops/s` | `1486.241 ops/s` | `+0.00%` | `neutral` |
