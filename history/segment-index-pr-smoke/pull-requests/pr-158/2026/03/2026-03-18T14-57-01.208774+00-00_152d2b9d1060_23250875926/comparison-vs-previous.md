# Benchmark Comparison

- Profile: `segment-index-pr-smoke`
- Baseline SHA: `52b04525c10e1f18faddd3a3d7f018427f1a5b53`
- Candidate SHA: `152d2b9d10603ba7775847bd8345134631dcf1bf`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `segment-index-get-multisegment-hot:getHitAsyncJoin` | `-` | `95.290 ops/s` | `-` | `new` |
| `segment-index-get-multisegment-hot:getHitSync` | `-` | `97.810 ops/s` | `-` | `new` |
| `segment-index-get-multisegment-hot:getMissAsyncJoin` | `-` | `178004.011 ops/s` | `-` | `new` |
| `segment-index-get-multisegment-hot:getMissSync` | `-` | `6364171.430 ops/s` | `-` | `new` |
| `segment-index-get-overlay:getHitAsyncJoin` | `160151.872 ops/s` | `171961.525 ops/s` | `+7.37%` | `better` |
| `segment-index-get-overlay:getHitSync` | `4729456.975 ops/s` | `5385093.759 ops/s` | `+13.86%` | `better` |
| `segment-index-get-overlay:getMissAsyncJoin` | `-` | `169894.948 ops/s` | `-` | `new` |
| `segment-index-get-overlay:getMissSync` | `-` | `6900567.452 ops/s` | `-` | `new` |
| `segment-index-get-persisted:getHitAsyncJoin` | `54557.081 ops/s` | `59646.187 ops/s` | `+9.33%` | `better` |
| `segment-index-get-persisted:getHitSync` | `103784.125 ops/s` | `108955.045 ops/s` | `+4.98%` | `better` |
| `segment-index-get-persisted:getMissAsyncJoin` | `-` | `167720.968 ops/s` | `-` | `new` |
| `segment-index-get-persisted:getMissSync` | `-` | `6744583.278 ops/s` | `-` | `new` |
| `segment-index-mixed-drain:partitionedIngestMixed` | `448575.461 ops/s` | `431841.555 ops/s` | `-3.73%` | `warning` |
| `segment-index-mixed-drain:partitionedIngestMixed:getWorkload` | `442786.229 ops/s` | `425952.638 ops/s` | `-3.80%` | `warning` |
| `segment-index-mixed-drain:partitionedIngestMixed:putWorkload` | `5789.232 ops/s` | `5888.917 ops/s` | `+1.72%` | `neutral` |
