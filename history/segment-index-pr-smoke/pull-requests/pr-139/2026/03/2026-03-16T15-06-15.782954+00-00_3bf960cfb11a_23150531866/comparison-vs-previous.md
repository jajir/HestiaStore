# Benchmark Comparison

- Profile: `segment-index-pr-smoke`
- Baseline SHA: `52b04525c10e1f18faddd3a3d7f018427f1a5b53`
- Candidate SHA: `3bf960cfb11ab1388c490dc13d04fc3e2ee46749`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `segment-index-get-multisegment-hot:getHitAsyncJoin` | `-` | `98.273 ops/s` | `-` | `new` |
| `segment-index-get-multisegment-hot:getHitSync` | `-` | `99.271 ops/s` | `-` | `new` |
| `segment-index-get-multisegment-hot:getMissAsyncJoin` | `-` | `178227.342 ops/s` | `-` | `new` |
| `segment-index-get-multisegment-hot:getMissSync` | `-` | `6173022.217 ops/s` | `-` | `new` |
| `segment-index-get-overlay:getHitAsyncJoin` | `160151.872 ops/s` | `173503.287 ops/s` | `+8.34%` | `better` |
| `segment-index-get-overlay:getHitSync` | `4729456.975 ops/s` | `5158898.220 ops/s` | `+9.08%` | `better` |
| `segment-index-get-overlay:getMissAsyncJoin` | `-` | `169530.382 ops/s` | `-` | `new` |
| `segment-index-get-overlay:getMissSync` | `-` | `6650877.814 ops/s` | `-` | `new` |
| `segment-index-get-persisted:getHitAsyncJoin` | `54557.081 ops/s` | `57090.405 ops/s` | `+4.64%` | `better` |
| `segment-index-get-persisted:getHitSync` | `103784.125 ops/s` | `93611.761 ops/s` | `-9.80%` | `worse` |
| `segment-index-get-persisted:getMissAsyncJoin` | `-` | `175499.987 ops/s` | `-` | `new` |
| `segment-index-get-persisted:getMissSync` | `-` | `6790269.881 ops/s` | `-` | `new` |
| `segment-index-mixed-drain:partitionedIngestMixed` | `448575.461 ops/s` | `440290.868 ops/s` | `-1.85%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed:getWorkload` | `442786.229 ops/s` | `434557.755 ops/s` | `-1.86%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed:putWorkload` | `5789.232 ops/s` | `5733.113 ops/s` | `-0.97%` | `neutral` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed` | `-` | `207609.576 ops/s` | `-` | `new` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:getWorkload` | `-` | `205009.165 ops/s` | `-` | `new` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:putWorkload` | `-` | `2600.411 ops/s` | `-` | `new` |
