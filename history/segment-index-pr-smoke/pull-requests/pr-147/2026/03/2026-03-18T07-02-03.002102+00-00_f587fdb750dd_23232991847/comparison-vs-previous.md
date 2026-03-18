# Benchmark Comparison

- Profile: `segment-index-pr-smoke`
- Baseline SHA: `52b04525c10e1f18faddd3a3d7f018427f1a5b53`
- Candidate SHA: `f587fdb750dd38c138778252bd0ec5eaf7704f15`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `segment-index-get-multisegment-hot:getHitAsyncJoin` | `-` | `105.140 ops/s` | `-` | `new` |
| `segment-index-get-multisegment-hot:getHitSync` | `-` | `89.951 ops/s` | `-` | `new` |
| `segment-index-get-multisegment-hot:getMissAsyncJoin` | `-` | `167335.731 ops/s` | `-` | `new` |
| `segment-index-get-multisegment-hot:getMissSync` | `-` | `6069185.654 ops/s` | `-` | `new` |
| `segment-index-get-overlay:getHitAsyncJoin` | `160151.872 ops/s` | `164139.704 ops/s` | `+2.49%` | `neutral` |
| `segment-index-get-overlay:getHitSync` | `4729456.975 ops/s` | `5642552.130 ops/s` | `+19.31%` | `better` |
| `segment-index-get-overlay:getMissAsyncJoin` | `-` | `166955.493 ops/s` | `-` | `new` |
| `segment-index-get-overlay:getMissSync` | `-` | `6833977.001 ops/s` | `-` | `new` |
| `segment-index-get-persisted:getHitAsyncJoin` | `54557.081 ops/s` | `57369.751 ops/s` | `+5.16%` | `better` |
| `segment-index-get-persisted:getHitSync` | `103784.125 ops/s` | `102086.044 ops/s` | `-1.64%` | `neutral` |
| `segment-index-get-persisted:getMissAsyncJoin` | `-` | `165585.818 ops/s` | `-` | `new` |
| `segment-index-get-persisted:getMissSync` | `-` | `7199861.959 ops/s` | `-` | `new` |
| `segment-index-mixed-drain:partitionedIngestMixed` | `448575.461 ops/s` | `384476.400 ops/s` | `-14.29%` | `worse` |
| `segment-index-mixed-drain:partitionedIngestMixed:getWorkload` | `442786.229 ops/s` | `378934.315 ops/s` | `-14.42%` | `worse` |
| `segment-index-mixed-drain:partitionedIngestMixed:putWorkload` | `5789.232 ops/s` | `5542.085 ops/s` | `-4.27%` | `warning` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed` | `-` | `208634.239 ops/s` | `-` | `new` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:getWorkload` | `-` | `205928.338 ops/s` | `-` | `new` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:putWorkload` | `-` | `2705.901 ops/s` | `-` | `new` |
