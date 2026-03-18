# Benchmark Comparison

- Profile: `segment-index-pr-smoke`
- Baseline SHA: `52b04525c10e1f18faddd3a3d7f018427f1a5b53`
- Candidate SHA: `67ecf099e47860f6b644e6e59ef58bc83f0c7dda`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `segment-index-get-multisegment-hot:getHitAsyncJoin` | `-` | `102.561 ops/s` | `-` | `new` |
| `segment-index-get-multisegment-hot:getHitSync` | `-` | `91.439 ops/s` | `-` | `new` |
| `segment-index-get-multisegment-hot:getMissAsyncJoin` | `-` | `165582.466 ops/s` | `-` | `new` |
| `segment-index-get-multisegment-hot:getMissSync` | `-` | `6392240.576 ops/s` | `-` | `new` |
| `segment-index-get-overlay:getHitAsyncJoin` | `160151.872 ops/s` | `162623.477 ops/s` | `+1.54%` | `neutral` |
| `segment-index-get-overlay:getHitSync` | `4729456.975 ops/s` | `5300023.447 ops/s` | `+12.06%` | `better` |
| `segment-index-get-overlay:getMissAsyncJoin` | `-` | `166562.820 ops/s` | `-` | `new` |
| `segment-index-get-overlay:getMissSync` | `-` | `7243780.508 ops/s` | `-` | `new` |
| `segment-index-get-persisted:getHitAsyncJoin` | `54557.081 ops/s` | `55597.332 ops/s` | `+1.91%` | `neutral` |
| `segment-index-get-persisted:getHitSync` | `103784.125 ops/s` | `112978.728 ops/s` | `+8.86%` | `better` |
| `segment-index-get-persisted:getMissAsyncJoin` | `-` | `165495.705 ops/s` | `-` | `new` |
| `segment-index-get-persisted:getMissSync` | `-` | `7092380.201 ops/s` | `-` | `new` |
| `segment-index-mixed-drain:partitionedIngestMixed` | `448575.461 ops/s` | `449385.987 ops/s` | `+0.18%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed:getWorkload` | `442786.229 ops/s` | `443503.562 ops/s` | `+0.16%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed:putWorkload` | `5789.232 ops/s` | `5882.425 ops/s` | `+1.61%` | `neutral` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed` | `-` | `207732.791 ops/s` | `-` | `new` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:getWorkload` | `-` | `205154.896 ops/s` | `-` | `new` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:putWorkload` | `-` | `2577.895 ops/s` | `-` | `new` |
