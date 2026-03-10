# Benchmark Comparison

- Bootstrap mode: no prior canonical baseline exists yet, and fallback git baseline predates the benchmark harness.
- Candidate results below are the first canonical benchmark snapshot for this workflow.


- Profile: `segment-index-pr-smoke`
- Baseline SHA: `52b04525c10e1f18faddd3a3d7f018427f1a5b53`
- Candidate SHA: `52b04525c10e1f18faddd3a3d7f018427f1a5b53`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `segment-index-get-overlay:getHitAsyncJoin` | `160151.872 ops/s` | `160151.872 ops/s` | `+0.00%` | `neutral` |
| `segment-index-get-overlay:getHitSync` | `4729456.975 ops/s` | `4729456.975 ops/s` | `+0.00%` | `neutral` |
| `segment-index-get-persisted:getHitAsyncJoin` | `54557.081 ops/s` | `54557.081 ops/s` | `+0.00%` | `neutral` |
| `segment-index-get-persisted:getHitSync` | `103784.125 ops/s` | `103784.125 ops/s` | `+0.00%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed` | `448575.461 ops/s` | `448575.461 ops/s` | `+0.00%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed:getWorkload` | `442786.229 ops/s` | `442786.229 ops/s` | `+0.00%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed:putWorkload` | `5789.232 ops/s` | `5789.232 ops/s` | `+0.00%` | `neutral` |
