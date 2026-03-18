# Benchmark Comparison

- Profile: `segment-index-pr-smoke`
- Baseline SHA: `52b04525c10e1f18faddd3a3d7f018427f1a5b53`
- Candidate SHA: `76b233d70f4f3076b3d0fad93bf7df8e83b5a129`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `segment-index-get-multisegment-hot:getHitAsyncJoin` | `-` | `101.371 ops/s` | `-` | `new` |
| `segment-index-get-multisegment-hot:getHitSync` | `-` | `94.906 ops/s` | `-` | `new` |
| `segment-index-get-multisegment-hot:getMissAsyncJoin` | `-` | `175830.438 ops/s` | `-` | `new` |
| `segment-index-get-multisegment-hot:getMissSync` | `-` | `6606001.074 ops/s` | `-` | `new` |
| `segment-index-get-overlay:getHitAsyncJoin` | `160151.872 ops/s` | `172807.768 ops/s` | `+7.90%` | `better` |
| `segment-index-get-overlay:getHitSync` | `4729456.975 ops/s` | `5410376.706 ops/s` | `+14.40%` | `better` |
| `segment-index-get-overlay:getMissAsyncJoin` | `-` | `177237.000 ops/s` | `-` | `new` |
| `segment-index-get-overlay:getMissSync` | `-` | `6935912.773 ops/s` | `-` | `new` |
| `segment-index-get-persisted:getHitAsyncJoin` | `54557.081 ops/s` | `57227.895 ops/s` | `+4.90%` | `better` |
| `segment-index-get-persisted:getHitSync` | `103784.125 ops/s` | `106314.625 ops/s` | `+2.44%` | `neutral` |
| `segment-index-get-persisted:getMissAsyncJoin` | `-` | `174659.872 ops/s` | `-` | `new` |
| `segment-index-get-persisted:getMissSync` | `-` | `6599698.896 ops/s` | `-` | `new` |
| `segment-index-mixed-drain:partitionedIngestMixed` | `448575.461 ops/s` | `440502.094 ops/s` | `-1.80%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed:getWorkload` | `442786.229 ops/s` | `434593.074 ops/s` | `-1.85%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed:putWorkload` | `5789.232 ops/s` | `5909.020 ops/s` | `+2.07%` | `neutral` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed` | `-` | `214295.367 ops/s` | `-` | `new` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:getWorkload` | `-` | `211415.847 ops/s` | `-` | `new` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:putWorkload` | `-` | `2879.520 ops/s` | `-` | `new` |
