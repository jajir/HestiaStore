# Benchmark Comparison

- Profile: `segment-index-pr-smoke`
- Baseline SHA: `52b04525c10e1f18faddd3a3d7f018427f1a5b53`
- Candidate SHA: `0e8681d482e73e2fce70969a31ddf73df595039e`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `segment-index-get-multisegment-hot:getHitAsyncJoin` | `-` | `94.286 ops/s` | `-` | `new` |
| `segment-index-get-multisegment-hot:getHitSync` | `-` | `94.255 ops/s` | `-` | `new` |
| `segment-index-get-multisegment-hot:getMissAsyncJoin` | `-` | `162357.634 ops/s` | `-` | `new` |
| `segment-index-get-multisegment-hot:getMissSync` | `-` | `4161588.708 ops/s` | `-` | `new` |
| `segment-index-get-overlay:getHitAsyncJoin` | `160151.872 ops/s` | `160776.366 ops/s` | `+0.39%` | `neutral` |
| `segment-index-get-overlay:getHitSync` | `4729456.975 ops/s` | `4246367.494 ops/s` | `-10.21%` | `worse` |
| `segment-index-get-overlay:getMissAsyncJoin` | `-` | `164804.351 ops/s` | `-` | `new` |
| `segment-index-get-overlay:getMissSync` | `-` | `4071563.357 ops/s` | `-` | `new` |
| `segment-index-get-persisted:getHitAsyncJoin` | `54557.081 ops/s` | `55294.950 ops/s` | `+1.35%` | `neutral` |
| `segment-index-get-persisted:getHitSync` | `103784.125 ops/s` | `110780.146 ops/s` | `+6.74%` | `better` |
| `segment-index-get-persisted:getMissAsyncJoin` | `-` | `161904.499 ops/s` | `-` | `new` |
| `segment-index-get-persisted:getMissSync` | `-` | `4005846.074 ops/s` | `-` | `new` |
| `segment-index-mixed-drain:partitionedIngestMixed` | `448575.461 ops/s` | `432190.499 ops/s` | `-3.65%` | `warning` |
| `segment-index-mixed-drain:partitionedIngestMixed:getWorkload` | `442786.229 ops/s` | `426329.409 ops/s` | `-3.72%` | `warning` |
| `segment-index-mixed-drain:partitionedIngestMixed:putWorkload` | `5789.232 ops/s` | `5861.091 ops/s` | `+1.24%` | `neutral` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed` | `-` | `217094.128 ops/s` | `-` | `new` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:getWorkload` | `-` | `214623.055 ops/s` | `-` | `new` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:putWorkload` | `-` | `2471.073 ops/s` | `-` | `new` |
