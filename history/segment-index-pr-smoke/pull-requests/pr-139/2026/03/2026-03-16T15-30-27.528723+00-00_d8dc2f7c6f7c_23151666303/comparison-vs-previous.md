# Benchmark Comparison

- Profile: `segment-index-pr-smoke`
- Baseline SHA: `52b04525c10e1f18faddd3a3d7f018427f1a5b53`
- Candidate SHA: `d8dc2f7c6f7c2ea8910f6d668cbe0d3bc09f41c4`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `segment-index-get-multisegment-hot:getHitAsyncJoin` | `-` | `102.587 ops/s` | `-` | `new` |
| `segment-index-get-multisegment-hot:getHitSync` | `-` | `89.835 ops/s` | `-` | `new` |
| `segment-index-get-multisegment-hot:getMissAsyncJoin` | `-` | `165402.218 ops/s` | `-` | `new` |
| `segment-index-get-multisegment-hot:getMissSync` | `-` | `6374263.620 ops/s` | `-` | `new` |
| `segment-index-get-overlay:getHitAsyncJoin` | `160151.872 ops/s` | `162454.847 ops/s` | `+1.44%` | `neutral` |
| `segment-index-get-overlay:getHitSync` | `4729456.975 ops/s` | `5296013.880 ops/s` | `+11.98%` | `better` |
| `segment-index-get-overlay:getMissAsyncJoin` | `-` | `165236.312 ops/s` | `-` | `new` |
| `segment-index-get-overlay:getMissSync` | `-` | `6731188.085 ops/s` | `-` | `new` |
| `segment-index-get-persisted:getHitAsyncJoin` | `54557.081 ops/s` | `55489.402 ops/s` | `+1.71%` | `neutral` |
| `segment-index-get-persisted:getHitSync` | `103784.125 ops/s` | `105053.993 ops/s` | `+1.22%` | `neutral` |
| `segment-index-get-persisted:getMissAsyncJoin` | `-` | `158637.720 ops/s` | `-` | `new` |
| `segment-index-get-persisted:getMissSync` | `-` | `7245799.362 ops/s` | `-` | `new` |
| `segment-index-mixed-drain:partitionedIngestMixed` | `448575.461 ops/s` | `383730.642 ops/s` | `-14.46%` | `worse` |
| `segment-index-mixed-drain:partitionedIngestMixed:getWorkload` | `442786.229 ops/s` | `378095.523 ops/s` | `-14.61%` | `worse` |
| `segment-index-mixed-drain:partitionedIngestMixed:putWorkload` | `5789.232 ops/s` | `5635.119 ops/s` | `-2.66%` | `neutral` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed` | `-` | `190849.931 ops/s` | `-` | `new` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:getWorkload` | `-` | `188173.552 ops/s` | `-` | `new` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:putWorkload` | `-` | `2676.379 ops/s` | `-` | `new` |
