# Benchmark Comparison

- Profile: `segment-index-pr-smoke`
- Baseline SHA: `52b04525c10e1f18faddd3a3d7f018427f1a5b53`
- Candidate SHA: `14ff6fc08747435c88e05382bfd45aad108f3202`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `segment-index-get-multisegment-hot:getHitAsyncJoin` | `-` | `97.358 ops/s` | `-` | `new` |
| `segment-index-get-multisegment-hot:getHitSync` | `-` | `98.740 ops/s` | `-` | `new` |
| `segment-index-get-multisegment-hot:getMissAsyncJoin` | `-` | `169454.677 ops/s` | `-` | `new` |
| `segment-index-get-multisegment-hot:getMissSync` | `-` | `6383455.631 ops/s` | `-` | `new` |
| `segment-index-get-overlay:getHitAsyncJoin` | `160151.872 ops/s` | `170902.317 ops/s` | `+6.71%` | `better` |
| `segment-index-get-overlay:getHitSync` | `4729456.975 ops/s` | `5164549.043 ops/s` | `+9.20%` | `better` |
| `segment-index-get-overlay:getMissAsyncJoin` | `-` | `175826.787 ops/s` | `-` | `new` |
| `segment-index-get-overlay:getMissSync` | `-` | `6606615.679 ops/s` | `-` | `new` |
| `segment-index-get-persisted:getHitAsyncJoin` | `54557.081 ops/s` | `57762.504 ops/s` | `+5.88%` | `better` |
| `segment-index-get-persisted:getHitSync` | `103784.125 ops/s` | `102811.247 ops/s` | `-0.94%` | `neutral` |
| `segment-index-get-persisted:getMissAsyncJoin` | `-` | `173645.575 ops/s` | `-` | `new` |
| `segment-index-get-persisted:getMissSync` | `-` | `6941634.716 ops/s` | `-` | `new` |
| `segment-index-mixed-drain:partitionedIngestMixed` | `448575.461 ops/s` | `399420.046 ops/s` | `-10.96%` | `worse` |
| `segment-index-mixed-drain:partitionedIngestMixed:getWorkload` | `442786.229 ops/s` | `393777.060 ops/s` | `-11.07%` | `worse` |
| `segment-index-mixed-drain:partitionedIngestMixed:putWorkload` | `5789.232 ops/s` | `5642.986 ops/s` | `-2.53%` | `neutral` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed` | `-` | `196303.735 ops/s` | `-` | `new` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:getWorkload` | `-` | `193842.012 ops/s` | `-` | `new` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:putWorkload` | `-` | `2461.723 ops/s` | `-` | `new` |
