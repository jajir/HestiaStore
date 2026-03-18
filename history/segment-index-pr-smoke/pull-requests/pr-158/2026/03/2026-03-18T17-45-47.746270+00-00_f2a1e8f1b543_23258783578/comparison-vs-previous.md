# Benchmark Comparison

- Profile: `segment-index-pr-smoke`
- Baseline SHA: `52b04525c10e1f18faddd3a3d7f018427f1a5b53`
- Candidate SHA: `f2a1e8f1b54313c02ae09c02881faaf11a151d42`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `segment-index-get-multisegment-hot:getHitAsyncJoin` | `-` | `98.338 ops/s` | `-` | `new` |
| `segment-index-get-multisegment-hot:getHitSync` | `-` | `91.249 ops/s` | `-` | `new` |
| `segment-index-get-multisegment-hot:getMissAsyncJoin` | `-` | `166211.786 ops/s` | `-` | `new` |
| `segment-index-get-multisegment-hot:getMissSync` | `-` | `7005842.298 ops/s` | `-` | `new` |
| `segment-index-get-overlay:getHitAsyncJoin` | `160151.872 ops/s` | `163997.344 ops/s` | `+2.40%` | `neutral` |
| `segment-index-get-overlay:getHitSync` | `4729456.975 ops/s` | `5080249.607 ops/s` | `+7.42%` | `better` |
| `segment-index-get-overlay:getMissAsyncJoin` | `-` | `156953.690 ops/s` | `-` | `new` |
| `segment-index-get-overlay:getMissSync` | `-` | `6711339.911 ops/s` | `-` | `new` |
| `segment-index-get-persisted:getHitAsyncJoin` | `54557.081 ops/s` | `56503.662 ops/s` | `+3.57%` | `better` |
| `segment-index-get-persisted:getHitSync` | `103784.125 ops/s` | `111786.106 ops/s` | `+7.71%` | `better` |
| `segment-index-get-persisted:getMissAsyncJoin` | `-` | `165710.523 ops/s` | `-` | `new` |
| `segment-index-get-persisted:getMissSync` | `-` | `6699681.095 ops/s` | `-` | `new` |
| `segment-index-mixed-drain:partitionedIngestMixed` | `448575.461 ops/s` | `449044.399 ops/s` | `+0.10%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed:getWorkload` | `442786.229 ops/s` | `442919.663 ops/s` | `+0.03%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed:putWorkload` | `5789.232 ops/s` | `6124.736 ops/s` | `+5.80%` | `better` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed` | `-` | `200114.036 ops/s` | `-` | `new` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:getWorkload` | `-` | `197280.479 ops/s` | `-` | `new` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:putWorkload` | `-` | `2833.557 ops/s` | `-` | `new` |
