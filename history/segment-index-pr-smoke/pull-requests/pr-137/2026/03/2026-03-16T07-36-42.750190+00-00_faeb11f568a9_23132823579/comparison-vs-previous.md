# Benchmark Comparison

- Profile: `segment-index-pr-smoke`
- Baseline SHA: `52b04525c10e1f18faddd3a3d7f018427f1a5b53`
- Candidate SHA: `faeb11f568a9f25cac50f970460ce0b5b659048b`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `segment-index-get-multisegment-hot:getHitAsyncJoin` | `-` | `100.226 ops/s` | `-` | `new` |
| `segment-index-get-multisegment-hot:getHitSync` | `-` | `97.749 ops/s` | `-` | `new` |
| `segment-index-get-multisegment-hot:getMissAsyncJoin` | `-` | `176561.005 ops/s` | `-` | `new` |
| `segment-index-get-multisegment-hot:getMissSync` | `-` | `6307918.846 ops/s` | `-` | `new` |
| `segment-index-get-overlay:getHitAsyncJoin` | `160151.872 ops/s` | `168328.193 ops/s` | `+5.11%` | `better` |
| `segment-index-get-overlay:getHitSync` | `4729456.975 ops/s` | `5311434.421 ops/s` | `+12.31%` | `better` |
| `segment-index-get-overlay:getMissAsyncJoin` | `-` | `178410.156 ops/s` | `-` | `new` |
| `segment-index-get-overlay:getMissSync` | `-` | `6953959.965 ops/s` | `-` | `new` |
| `segment-index-get-persisted:getHitAsyncJoin` | `54557.081 ops/s` | `59139.203 ops/s` | `+8.40%` | `better` |
| `segment-index-get-persisted:getHitSync` | `103784.125 ops/s` | `103404.933 ops/s` | `-0.37%` | `neutral` |
| `segment-index-get-persisted:getMissAsyncJoin` | `-` | `170665.531 ops/s` | `-` | `new` |
| `segment-index-get-persisted:getMissSync` | `-` | `7083504.335 ops/s` | `-` | `new` |
| `segment-index-mixed-drain:partitionedIngestMixed` | `448575.461 ops/s` | `437948.719 ops/s` | `-2.37%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed:getWorkload` | `442786.229 ops/s` | `432130.700 ops/s` | `-2.41%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed:putWorkload` | `5789.232 ops/s` | `5818.019 ops/s` | `+0.50%` | `neutral` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed` | `-` | `192041.545 ops/s` | `-` | `new` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:getWorkload` | `-` | `189396.172 ops/s` | `-` | `new` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:putWorkload` | `-` | `2645.373 ops/s` | `-` | `new` |
