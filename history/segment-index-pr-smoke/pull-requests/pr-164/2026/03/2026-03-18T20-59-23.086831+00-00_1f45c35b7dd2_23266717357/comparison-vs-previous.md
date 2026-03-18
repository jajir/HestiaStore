# Benchmark Comparison

- Profile: `segment-index-pr-smoke`
- Baseline SHA: `52b04525c10e1f18faddd3a3d7f018427f1a5b53`
- Candidate SHA: `1f45c35b7dd27fb8d6fcd6efc9880e2b927322f4`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `segment-index-get-multisegment-hot:getHitAsyncJoin` | `-` | `101.006 ops/s` | `-` | `new` |
| `segment-index-get-multisegment-hot:getHitSync` | `-` | `94.378 ops/s` | `-` | `new` |
| `segment-index-get-multisegment-hot:getMissAsyncJoin` | `-` | `165138.216 ops/s` | `-` | `new` |
| `segment-index-get-multisegment-hot:getMissSync` | `-` | `6744391.127 ops/s` | `-` | `new` |
| `segment-index-get-overlay:getHitAsyncJoin` | `160151.872 ops/s` | `164856.192 ops/s` | `+2.94%` | `neutral` |
| `segment-index-get-overlay:getHitSync` | `4729456.975 ops/s` | `5374076.699 ops/s` | `+13.63%` | `better` |
| `segment-index-get-overlay:getMissAsyncJoin` | `-` | `168301.592 ops/s` | `-` | `new` |
| `segment-index-get-overlay:getMissSync` | `-` | `7249084.417 ops/s` | `-` | `new` |
| `segment-index-get-persisted:getHitAsyncJoin` | `54557.081 ops/s` | `57911.880 ops/s` | `+6.15%` | `better` |
| `segment-index-get-persisted:getHitSync` | `103784.125 ops/s` | `108832.590 ops/s` | `+4.86%` | `better` |
| `segment-index-get-persisted:getMissAsyncJoin` | `-` | `166800.021 ops/s` | `-` | `new` |
| `segment-index-get-persisted:getMissSync` | `-` | `7210878.785 ops/s` | `-` | `new` |
| `segment-index-mixed-drain:partitionedIngestMixed` | `448575.461 ops/s` | `465785.329 ops/s` | `+3.84%` | `better` |
| `segment-index-mixed-drain:partitionedIngestMixed:getWorkload` | `442786.229 ops/s` | `459634.021 ops/s` | `+3.80%` | `better` |
| `segment-index-mixed-drain:partitionedIngestMixed:putWorkload` | `5789.232 ops/s` | `6151.308 ops/s` | `+6.25%` | `better` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed` | `-` | `203197.876 ops/s` | `-` | `new` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:getWorkload` | `-` | `200458.976 ops/s` | `-` | `new` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:putWorkload` | `-` | `2738.900 ops/s` | `-` | `new` |
