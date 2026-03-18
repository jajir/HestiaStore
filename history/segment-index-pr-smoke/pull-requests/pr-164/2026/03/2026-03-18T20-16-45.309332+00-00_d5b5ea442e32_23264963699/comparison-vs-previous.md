# Benchmark Comparison

- Profile: `segment-index-pr-smoke`
- Baseline SHA: `52b04525c10e1f18faddd3a3d7f018427f1a5b53`
- Candidate SHA: `d5b5ea442e32c4e0a70c65164d991d84a28dd8bb`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `segment-index-get-multisegment-hot:getHitAsyncJoin` | `-` | `104.992 ops/s` | `-` | `new` |
| `segment-index-get-multisegment-hot:getHitSync` | `-` | `88.549 ops/s` | `-` | `new` |
| `segment-index-get-multisegment-hot:getMissAsyncJoin` | `-` | `176811.483 ops/s` | `-` | `new` |
| `segment-index-get-multisegment-hot:getMissSync` | `-` | `6614902.627 ops/s` | `-` | `new` |
| `segment-index-get-overlay:getHitAsyncJoin` | `160151.872 ops/s` | `172861.692 ops/s` | `+7.94%` | `better` |
| `segment-index-get-overlay:getHitSync` | `4729456.975 ops/s` | `5100967.276 ops/s` | `+7.86%` | `better` |
| `segment-index-get-overlay:getMissAsyncJoin` | `-` | `171861.226 ops/s` | `-` | `new` |
| `segment-index-get-overlay:getMissSync` | `-` | `7054049.639 ops/s` | `-` | `new` |
| `segment-index-get-persisted:getHitAsyncJoin` | `54557.081 ops/s` | `58422.204 ops/s` | `+7.08%` | `better` |
| `segment-index-get-persisted:getHitSync` | `103784.125 ops/s` | `105763.517 ops/s` | `+1.91%` | `neutral` |
| `segment-index-get-persisted:getMissAsyncJoin` | `-` | `176881.293 ops/s` | `-` | `new` |
| `segment-index-get-persisted:getMissSync` | `-` | `6886292.944 ops/s` | `-` | `new` |
| `segment-index-mixed-drain:partitionedIngestMixed` | `448575.461 ops/s` | `437167.460 ops/s` | `-2.54%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed:getWorkload` | `442786.229 ops/s` | `431307.269 ops/s` | `-2.59%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed:putWorkload` | `5789.232 ops/s` | `5860.191 ops/s` | `+1.23%` | `neutral` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed` | `-` | `194305.020 ops/s` | `-` | `new` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:getWorkload` | `-` | `191647.069 ops/s` | `-` | `new` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:putWorkload` | `-` | `2657.952 ops/s` | `-` | `new` |
