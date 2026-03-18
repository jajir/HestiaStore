# Benchmark Comparison

- Profile: `segment-index-pr-smoke`
- Baseline SHA: `52b04525c10e1f18faddd3a3d7f018427f1a5b53`
- Candidate SHA: `6237788bb7a10504053ec25327351fb6f14491c9`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `segment-index-get-multisegment-hot:getHitAsyncJoin` | `-` | `98.945 ops/s` | `-` | `new` |
| `segment-index-get-multisegment-hot:getHitSync` | `-` | `108.993 ops/s` | `-` | `new` |
| `segment-index-get-multisegment-hot:getMissAsyncJoin` | `-` | `177392.286 ops/s` | `-` | `new` |
| `segment-index-get-multisegment-hot:getMissSync` | `-` | `3792506.689 ops/s` | `-` | `new` |
| `segment-index-get-overlay:getHitAsyncJoin` | `160151.872 ops/s` | `167679.745 ops/s` | `+4.70%` | `better` |
| `segment-index-get-overlay:getHitSync` | `4729456.975 ops/s` | `4079898.511 ops/s` | `-13.73%` | `worse` |
| `segment-index-get-overlay:getMissAsyncJoin` | `-` | `164428.063 ops/s` | `-` | `new` |
| `segment-index-get-overlay:getMissSync` | `-` | `3961231.401 ops/s` | `-` | `new` |
| `segment-index-get-persisted:getHitAsyncJoin` | `54557.081 ops/s` | `57174.499 ops/s` | `+4.80%` | `better` |
| `segment-index-get-persisted:getHitSync` | `103784.125 ops/s` | `114606.124 ops/s` | `+10.43%` | `better` |
| `segment-index-get-persisted:getMissAsyncJoin` | `-` | `174434.570 ops/s` | `-` | `new` |
| `segment-index-get-persisted:getMissSync` | `-` | `3993051.580 ops/s` | `-` | `new` |
| `segment-index-mixed-drain:partitionedIngestMixed` | `448575.461 ops/s` | `444256.170 ops/s` | `-0.96%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed:getWorkload` | `442786.229 ops/s` | `438363.797 ops/s` | `-1.00%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed:putWorkload` | `5789.232 ops/s` | `5892.373 ops/s` | `+1.78%` | `neutral` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed` | `-` | `198155.247 ops/s` | `-` | `new` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:getWorkload` | `-` | `195234.964 ops/s` | `-` | `new` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:putWorkload` | `-` | `2920.282 ops/s` | `-` | `new` |
