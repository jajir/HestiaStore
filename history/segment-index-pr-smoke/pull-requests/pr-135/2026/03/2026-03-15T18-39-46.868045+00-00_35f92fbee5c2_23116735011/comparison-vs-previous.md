# Benchmark Comparison

- Profile: `segment-index-pr-smoke`
- Baseline SHA: `52b04525c10e1f18faddd3a3d7f018427f1a5b53`
- Candidate SHA: `35f92fbee5c2d684b3bda398160cae2aaf50bf21`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `segment-index-get-overlay:getHitAsyncJoin` | `160151.872 ops/s` | `217129.835 ops/s` | `+35.58%` | `better` |
| `segment-index-get-overlay:getHitSync` | `4729456.975 ops/s` | `3616858.339 ops/s` | `-23.52%` | `worse` |
| `segment-index-get-persisted:getHitAsyncJoin` | `54557.081 ops/s` | `73130.896 ops/s` | `+34.04%` | `better` |
| `segment-index-get-persisted:getHitSync` | `103784.125 ops/s` | `113755.301 ops/s` | `+9.61%` | `better` |
| `segment-index-mixed-drain:partitionedIngestMixed` | `448575.461 ops/s` | `452797.032 ops/s` | `+0.94%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed:getWorkload` | `442786.229 ops/s` | `447157.900 ops/s` | `+0.99%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed:putWorkload` | `5789.232 ops/s` | `5639.132 ops/s` | `-2.59%` | `neutral` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed` | `-` | `207008.866 ops/s` | `-` | `new` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:getWorkload` | `-` | `204332.376 ops/s` | `-` | `new` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:putWorkload` | `-` | `2676.490 ops/s` | `-` | `new` |
