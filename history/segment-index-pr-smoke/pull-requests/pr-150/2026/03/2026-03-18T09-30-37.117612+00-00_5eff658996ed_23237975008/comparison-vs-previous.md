# Benchmark Comparison

- Profile: `segment-index-pr-smoke`
- Baseline SHA: `52b04525c10e1f18faddd3a3d7f018427f1a5b53`
- Candidate SHA: `5eff658996edbf469f6c6a5952a6d77193493be2`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `segment-index-get-multisegment-hot:getHitAsyncJoin` | `-` | `101.948 ops/s` | `-` | `new` |
| `segment-index-get-multisegment-hot:getHitSync` | `-` | `95.801 ops/s` | `-` | `new` |
| `segment-index-get-multisegment-hot:getMissAsyncJoin` | `-` | `241640.310 ops/s` | `-` | `new` |
| `segment-index-get-multisegment-hot:getMissSync` | `-` | `4335917.966 ops/s` | `-` | `new` |
| `segment-index-get-overlay:getHitAsyncJoin` | `160151.872 ops/s` | `233189.046 ops/s` | `+45.60%` | `better` |
| `segment-index-get-overlay:getHitSync` | `4729456.975 ops/s` | `3713320.603 ops/s` | `-21.49%` | `worse` |
| `segment-index-get-overlay:getMissAsyncJoin` | `-` | `227534.935 ops/s` | `-` | `new` |
| `segment-index-get-overlay:getMissSync` | `-` | `4747866.608 ops/s` | `-` | `new` |
| `segment-index-get-persisted:getHitAsyncJoin` | `54557.081 ops/s` | `71189.527 ops/s` | `+30.49%` | `better` |
| `segment-index-get-persisted:getHitSync` | `103784.125 ops/s` | `109386.867 ops/s` | `+5.40%` | `better` |
| `segment-index-get-persisted:getMissAsyncJoin` | `-` | `232086.207 ops/s` | `-` | `new` |
| `segment-index-get-persisted:getMissSync` | `-` | `5103637.313 ops/s` | `-` | `new` |
| `segment-index-mixed-drain:partitionedIngestMixed` | `448575.461 ops/s` | `450159.552 ops/s` | `+0.35%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed:getWorkload` | `442786.229 ops/s` | `444276.616 ops/s` | `+0.34%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed:putWorkload` | `5789.232 ops/s` | `5882.936 ops/s` | `+1.62%` | `neutral` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed` | `-` | `207248.795 ops/s` | `-` | `new` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:getWorkload` | `-` | `204874.899 ops/s` | `-` | `new` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:putWorkload` | `-` | `2373.896 ops/s` | `-` | `new` |
