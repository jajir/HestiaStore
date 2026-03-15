# Benchmark Comparison

- Profile: `segment-index-pr-smoke`
- Baseline SHA: `52b04525c10e1f18faddd3a3d7f018427f1a5b53`
- Candidate SHA: `a1d8d2f5a054bab71763c86a486d30185f9d318a`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `segment-index-get-overlay:getHitAsyncJoin` | `160151.872 ops/s` | `226715.096 ops/s` | `+41.56%` | `better` |
| `segment-index-get-overlay:getHitSync` | `4729456.975 ops/s` | `3617474.509 ops/s` | `-23.51%` | `worse` |
| `segment-index-get-persisted:getHitAsyncJoin` | `54557.081 ops/s` | `74680.485 ops/s` | `+36.89%` | `better` |
| `segment-index-get-persisted:getHitSync` | `103784.125 ops/s` | `102696.986 ops/s` | `-1.05%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed` | `448575.461 ops/s` | `455293.520 ops/s` | `+1.50%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed:getWorkload` | `442786.229 ops/s` | `449635.059 ops/s` | `+1.55%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed:putWorkload` | `5789.232 ops/s` | `5658.462 ops/s` | `-2.26%` | `neutral` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed` | `-` | `187894.621 ops/s` | `-` | `new` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:getWorkload` | `-` | `185230.412 ops/s` | `-` | `new` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:putWorkload` | `-` | `2664.209 ops/s` | `-` | `new` |
