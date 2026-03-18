# Benchmark Comparison

- Profile: `segment-index-pr-smoke`
- Baseline SHA: `52b04525c10e1f18faddd3a3d7f018427f1a5b53`
- Candidate SHA: `ecf4c7fb5682562b17a4adca2c91ebea6cbf603e`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `segment-index-get-multisegment-hot:getHitAsyncJoin` | `-` | `87.316 ops/s` | `-` | `new` |
| `segment-index-get-multisegment-hot:getHitSync` | `-` | `86.723 ops/s` | `-` | `new` |
| `segment-index-get-multisegment-hot:getMissAsyncJoin` | `-` | `166984.300 ops/s` | `-` | `new` |
| `segment-index-get-multisegment-hot:getMissSync` | `-` | `3848695.940 ops/s` | `-` | `new` |
| `segment-index-get-overlay:getHitAsyncJoin` | `160151.872 ops/s` | `157950.230 ops/s` | `-1.37%` | `neutral` |
| `segment-index-get-overlay:getHitSync` | `4729456.975 ops/s` | `4127714.924 ops/s` | `-12.72%` | `worse` |
| `segment-index-get-overlay:getMissAsyncJoin` | `-` | `164124.787 ops/s` | `-` | `new` |
| `segment-index-get-overlay:getMissSync` | `-` | `3991452.497 ops/s` | `-` | `new` |
| `segment-index-get-persisted:getHitAsyncJoin` | `54557.081 ops/s` | `56629.138 ops/s` | `+3.80%` | `better` |
| `segment-index-get-persisted:getHitSync` | `103784.125 ops/s` | `107827.211 ops/s` | `+3.90%` | `better` |
| `segment-index-get-persisted:getMissAsyncJoin` | `-` | `161868.497 ops/s` | `-` | `new` |
| `segment-index-get-persisted:getMissSync` | `-` | `3899090.371 ops/s` | `-` | `new` |
| `segment-index-mixed-drain:partitionedIngestMixed` | `448575.461 ops/s` | `450466.119 ops/s` | `+0.42%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed:getWorkload` | `442786.229 ops/s` | `445225.120 ops/s` | `+0.55%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed:putWorkload` | `5789.232 ops/s` | `5240.999 ops/s` | `-9.47%` | `worse` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed` | `-` | `195656.069 ops/s` | `-` | `new` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:getWorkload` | `-` | `193048.262 ops/s` | `-` | `new` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:putWorkload` | `-` | `2607.808 ops/s` | `-` | `new` |
