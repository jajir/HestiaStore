# Benchmark Comparison

- Profile: `segment-index-pr-smoke`
- Baseline SHA: `52b04525c10e1f18faddd3a3d7f018427f1a5b53`
- Candidate SHA: `8073dd50199717b245e675dbf137de5e8d4e1350`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `segment-index-get-multisegment-hot:getHitAsyncJoin` | `-` | `98.605 ops/s` | `-` | `new` |
| `segment-index-get-multisegment-hot:getHitSync` | `-` | `94.878 ops/s` | `-` | `new` |
| `segment-index-get-multisegment-hot:getMissAsyncJoin` | `-` | `174955.349 ops/s` | `-` | `new` |
| `segment-index-get-multisegment-hot:getMissSync` | `-` | `6218668.290 ops/s` | `-` | `new` |
| `segment-index-get-overlay:getHitAsyncJoin` | `160151.872 ops/s` | `174855.706 ops/s` | `+9.18%` | `better` |
| `segment-index-get-overlay:getHitSync` | `4729456.975 ops/s` | `5306543.043 ops/s` | `+12.20%` | `better` |
| `segment-index-get-overlay:getMissAsyncJoin` | `-` | `175239.545 ops/s` | `-` | `new` |
| `segment-index-get-overlay:getMissSync` | `-` | `6801882.142 ops/s` | `-` | `new` |
| `segment-index-get-persisted:getHitAsyncJoin` | `54557.081 ops/s` | `57919.590 ops/s` | `+6.16%` | `better` |
| `segment-index-get-persisted:getHitSync` | `103784.125 ops/s` | `100016.656 ops/s` | `-3.63%` | `warning` |
| `segment-index-get-persisted:getMissAsyncJoin` | `-` | `174632.937 ops/s` | `-` | `new` |
| `segment-index-get-persisted:getMissSync` | `-` | `7323109.028 ops/s` | `-` | `new` |
| `segment-index-mixed-drain:partitionedIngestMixed` | `448575.461 ops/s` | `443227.699 ops/s` | `-1.19%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed:getWorkload` | `442786.229 ops/s` | `437409.500 ops/s` | `-1.21%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed:putWorkload` | `5789.232 ops/s` | `5818.198 ops/s` | `+0.50%` | `neutral` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed` | `-` | `210473.003 ops/s` | `-` | `new` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:getWorkload` | `-` | `207901.892 ops/s` | `-` | `new` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:putWorkload` | `-` | `2571.111 ops/s` | `-` | `new` |
