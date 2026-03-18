# Benchmark Comparison

- Profile: `segment-index-pr-smoke`
- Baseline SHA: `52b04525c10e1f18faddd3a3d7f018427f1a5b53`
- Candidate SHA: `be37bb33083d98ea01052accaad34b7343dfeb0f`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `segment-index-get-multisegment-hot:getHitAsyncJoin` | `-` | `99.638 ops/s` | `-` | `new` |
| `segment-index-get-multisegment-hot:getHitSync` | `-` | `95.232 ops/s` | `-` | `new` |
| `segment-index-get-multisegment-hot:getMissAsyncJoin` | `-` | `174825.776 ops/s` | `-` | `new` |
| `segment-index-get-multisegment-hot:getMissSync` | `-` | `6225662.277 ops/s` | `-` | `new` |
| `segment-index-get-overlay:getHitAsyncJoin` | `160151.872 ops/s` | `165359.084 ops/s` | `+3.25%` | `better` |
| `segment-index-get-overlay:getHitSync` | `4729456.975 ops/s` | `5204720.863 ops/s` | `+10.05%` | `better` |
| `segment-index-get-overlay:getMissAsyncJoin` | `-` | `173007.188 ops/s` | `-` | `new` |
| `segment-index-get-overlay:getMissSync` | `-` | `7059880.198 ops/s` | `-` | `new` |
| `segment-index-get-persisted:getHitAsyncJoin` | `54557.081 ops/s` | `56252.327 ops/s` | `+3.11%` | `better` |
| `segment-index-get-persisted:getHitSync` | `103784.125 ops/s` | `110193.109 ops/s` | `+6.18%` | `better` |
| `segment-index-get-persisted:getMissAsyncJoin` | `-` | `174037.618 ops/s` | `-` | `new` |
| `segment-index-get-persisted:getMissSync` | `-` | `6530099.340 ops/s` | `-` | `new` |
| `segment-index-mixed-drain:partitionedIngestMixed` | `448575.461 ops/s` | `437598.612 ops/s` | `-2.45%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed:getWorkload` | `442786.229 ops/s` | `431751.768 ops/s` | `-2.49%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed:putWorkload` | `5789.232 ops/s` | `5846.844 ops/s` | `+1.00%` | `neutral` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed` | `-` | `199167.163 ops/s` | `-` | `new` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:getWorkload` | `-` | `196370.017 ops/s` | `-` | `new` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:putWorkload` | `-` | `2797.146 ops/s` | `-` | `new` |
