# Benchmark Comparison

- Profile: `segment-index-pr-smoke`
- Baseline SHA: `52b04525c10e1f18faddd3a3d7f018427f1a5b53`
- Candidate SHA: `344e7e4152e1ad6684de35bafbed0194a8c96ca2`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `segment-index-get-multisegment-hot:getHitAsyncJoin` | `-` | `99.580 ops/s` | `-` | `new` |
| `segment-index-get-multisegment-hot:getHitSync` | `-` | `91.933 ops/s` | `-` | `new` |
| `segment-index-get-multisegment-hot:getMissAsyncJoin` | `-` | `178680.527 ops/s` | `-` | `new` |
| `segment-index-get-multisegment-hot:getMissSync` | `-` | `6938378.826 ops/s` | `-` | `new` |
| `segment-index-get-overlay:getHitAsyncJoin` | `160151.872 ops/s` | `172395.218 ops/s` | `+7.64%` | `better` |
| `segment-index-get-overlay:getHitSync` | `4729456.975 ops/s` | `5416841.714 ops/s` | `+14.53%` | `better` |
| `segment-index-get-overlay:getMissAsyncJoin` | `-` | `177390.217 ops/s` | `-` | `new` |
| `segment-index-get-overlay:getMissSync` | `-` | `7122360.886 ops/s` | `-` | `new` |
| `segment-index-get-persisted:getHitAsyncJoin` | `54557.081 ops/s` | `56545.410 ops/s` | `+3.64%` | `better` |
| `segment-index-get-persisted:getHitSync` | `103784.125 ops/s` | `111215.313 ops/s` | `+7.16%` | `better` |
| `segment-index-get-persisted:getMissAsyncJoin` | `-` | `167619.039 ops/s` | `-` | `new` |
| `segment-index-get-persisted:getMissSync` | `-` | `6408935.203 ops/s` | `-` | `new` |
| `segment-index-mixed-drain:partitionedIngestMixed` | `448575.461 ops/s` | `458883.986 ops/s` | `+2.30%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed:getWorkload` | `442786.229 ops/s` | `452819.061 ops/s` | `+2.27%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed:putWorkload` | `5789.232 ops/s` | `6064.926 ops/s` | `+4.76%` | `better` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed` | `-` | `186639.691 ops/s` | `-` | `new` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:getWorkload` | `-` | `184253.755 ops/s` | `-` | `new` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:putWorkload` | `-` | `2385.935 ops/s` | `-` | `new` |
