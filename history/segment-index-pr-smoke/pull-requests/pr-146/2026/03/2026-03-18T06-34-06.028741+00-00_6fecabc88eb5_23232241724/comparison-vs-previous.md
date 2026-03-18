# Benchmark Comparison

- Profile: `segment-index-pr-smoke`
- Baseline SHA: `52b04525c10e1f18faddd3a3d7f018427f1a5b53`
- Candidate SHA: `6fecabc88eb5513236a6bcdd589cce59d5153e0f`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `segment-index-get-multisegment-hot:getHitAsyncJoin` | `-` | `99.162 ops/s` | `-` | `new` |
| `segment-index-get-multisegment-hot:getHitSync` | `-` | `93.019 ops/s` | `-` | `new` |
| `segment-index-get-multisegment-hot:getMissAsyncJoin` | `-` | `232485.779 ops/s` | `-` | `new` |
| `segment-index-get-multisegment-hot:getMissSync` | `-` | `4751129.182 ops/s` | `-` | `new` |
| `segment-index-get-overlay:getHitAsyncJoin` | `160151.872 ops/s` | `229030.886 ops/s` | `+43.01%` | `better` |
| `segment-index-get-overlay:getHitSync` | `4729456.975 ops/s` | `3664482.263 ops/s` | `-22.52%` | `worse` |
| `segment-index-get-overlay:getMissAsyncJoin` | `-` | `226237.279 ops/s` | `-` | `new` |
| `segment-index-get-overlay:getMissSync` | `-` | `4743845.263 ops/s` | `-` | `new` |
| `segment-index-get-persisted:getHitAsyncJoin` | `54557.081 ops/s` | `71917.121 ops/s` | `+31.82%` | `better` |
| `segment-index-get-persisted:getHitSync` | `103784.125 ops/s` | `96445.528 ops/s` | `-7.07%` | `worse` |
| `segment-index-get-persisted:getMissAsyncJoin` | `-` | `221240.615 ops/s` | `-` | `new` |
| `segment-index-get-persisted:getMissSync` | `-` | `4943937.603 ops/s` | `-` | `new` |
| `segment-index-mixed-drain:partitionedIngestMixed` | `448575.461 ops/s` | `388639.444 ops/s` | `-13.36%` | `worse` |
| `segment-index-mixed-drain:partitionedIngestMixed:getWorkload` | `442786.229 ops/s` | `382959.089 ops/s` | `-13.51%` | `worse` |
| `segment-index-mixed-drain:partitionedIngestMixed:putWorkload` | `5789.232 ops/s` | `5680.355 ops/s` | `-1.88%` | `neutral` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed` | `-` | `162159.101 ops/s` | `-` | `new` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:getWorkload` | `-` | `159730.623 ops/s` | `-` | `new` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:putWorkload` | `-` | `2428.477 ops/s` | `-` | `new` |
