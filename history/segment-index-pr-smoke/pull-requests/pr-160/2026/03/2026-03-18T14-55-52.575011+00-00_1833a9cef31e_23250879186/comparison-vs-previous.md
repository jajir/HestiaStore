# Benchmark Comparison

- Profile: `segment-index-pr-smoke`
- Baseline SHA: `52b04525c10e1f18faddd3a3d7f018427f1a5b53`
- Candidate SHA: `1833a9cef31e1c72018191474abbc13deeb2e127`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `segment-index-get-multisegment-hot:getHitAsyncJoin` | `-` | `100.512 ops/s` | `-` | `new` |
| `segment-index-get-multisegment-hot:getHitSync` | `-` | `97.345 ops/s` | `-` | `new` |
| `segment-index-get-multisegment-hot:getMissAsyncJoin` | `-` | `176459.560 ops/s` | `-` | `new` |
| `segment-index-get-multisegment-hot:getMissSync` | `-` | `6483005.024 ops/s` | `-` | `new` |
| `segment-index-get-overlay:getHitAsyncJoin` | `160151.872 ops/s` | `169230.065 ops/s` | `+5.67%` | `better` |
| `segment-index-get-overlay:getHitSync` | `4729456.975 ops/s` | `5115096.319 ops/s` | `+8.15%` | `better` |
| `segment-index-get-overlay:getMissAsyncJoin` | `-` | `175715.028 ops/s` | `-` | `new` |
| `segment-index-get-overlay:getMissSync` | `-` | `7154845.233 ops/s` | `-` | `new` |
| `segment-index-get-persisted:getHitAsyncJoin` | `54557.081 ops/s` | `57740.482 ops/s` | `+5.83%` | `better` |
| `segment-index-get-persisted:getHitSync` | `103784.125 ops/s` | `102851.897 ops/s` | `-0.90%` | `neutral` |
| `segment-index-get-persisted:getMissAsyncJoin` | `-` | `175911.461 ops/s` | `-` | `new` |
| `segment-index-get-persisted:getMissSync` | `-` | `6248541.389 ops/s` | `-` | `new` |
| `segment-index-mixed-drain:partitionedIngestMixed` | `448575.461 ops/s` | `452455.391 ops/s` | `+0.86%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed:getWorkload` | `442786.229 ops/s` | `446590.758 ops/s` | `+0.86%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed:putWorkload` | `5789.232 ops/s` | `5864.633 ops/s` | `+1.30%` | `neutral` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed` | `-` | `192570.646 ops/s` | `-` | `new` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:getWorkload` | `-` | `189778.598 ops/s` | `-` | `new` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:putWorkload` | `-` | `2792.047 ops/s` | `-` | `new` |
