# Benchmark Comparison

- Profile: `segment-index-pr-smoke`
- Baseline SHA: `52b04525c10e1f18faddd3a3d7f018427f1a5b53`
- Candidate SHA: `01e59971ab8d5d634c192d608f933c6dafac66c0`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `segment-index-get-multisegment-hot:getHitAsyncJoin` | `-` | `98.872 ops/s` | `-` | `new` |
| `segment-index-get-multisegment-hot:getHitSync` | `-` | `92.729 ops/s` | `-` | `new` |
| `segment-index-get-multisegment-hot:getMissAsyncJoin` | `-` | `176388.313 ops/s` | `-` | `new` |
| `segment-index-get-multisegment-hot:getMissSync` | `-` | `6601501.288 ops/s` | `-` | `new` |
| `segment-index-get-overlay:getHitAsyncJoin` | `160151.872 ops/s` | `172209.110 ops/s` | `+7.53%` | `better` |
| `segment-index-get-overlay:getHitSync` | `4729456.975 ops/s` | `5385279.534 ops/s` | `+13.87%` | `better` |
| `segment-index-get-overlay:getMissAsyncJoin` | `-` | `176782.671 ops/s` | `-` | `new` |
| `segment-index-get-overlay:getMissSync` | `-` | `6816336.988 ops/s` | `-` | `new` |
| `segment-index-get-persisted:getHitAsyncJoin` | `54557.081 ops/s` | `57775.070 ops/s` | `+5.90%` | `better` |
| `segment-index-get-persisted:getHitSync` | `103784.125 ops/s` | `109274.338 ops/s` | `+5.29%` | `better` |
| `segment-index-get-persisted:getMissAsyncJoin` | `-` | `177237.951 ops/s` | `-` | `new` |
| `segment-index-get-persisted:getMissSync` | `-` | `7020069.652 ops/s` | `-` | `new` |
| `segment-index-mixed-drain:partitionedIngestMixed` | `448575.461 ops/s` | `468996.334 ops/s` | `+4.55%` | `better` |
| `segment-index-mixed-drain:partitionedIngestMixed:getWorkload` | `442786.229 ops/s` | `462998.704 ops/s` | `+4.56%` | `better` |
| `segment-index-mixed-drain:partitionedIngestMixed:putWorkload` | `5789.232 ops/s` | `5997.629 ops/s` | `+3.60%` | `better` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed` | `-` | `206591.881 ops/s` | `-` | `new` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:getWorkload` | `-` | `203984.695 ops/s` | `-` | `new` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:putWorkload` | `-` | `2607.185 ops/s` | `-` | `new` |
