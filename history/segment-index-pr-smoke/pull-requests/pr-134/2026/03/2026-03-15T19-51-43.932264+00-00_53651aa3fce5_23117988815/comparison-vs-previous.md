# Benchmark Comparison

- Profile: `segment-index-pr-smoke`
- Baseline SHA: `52b04525c10e1f18faddd3a3d7f018427f1a5b53`
- Candidate SHA: `53651aa3fce5af1a393520ccc19389bb1507e839`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `segment-index-get-overlay:getHitAsyncJoin` | `160151.872 ops/s` | `167627.673 ops/s` | `+4.67%` | `better` |
| `segment-index-get-overlay:getHitSync` | `4729456.975 ops/s` | `5135265.164 ops/s` | `+8.58%` | `better` |
| `segment-index-get-persisted:getHitAsyncJoin` | `54557.081 ops/s` | `58799.386 ops/s` | `+7.78%` | `better` |
| `segment-index-get-persisted:getHitSync` | `103784.125 ops/s` | `106893.908 ops/s` | `+3.00%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed` | `448575.461 ops/s` | `434423.373 ops/s` | `-3.15%` | `warning` |
| `segment-index-mixed-drain:partitionedIngestMixed:getWorkload` | `442786.229 ops/s` | `428568.376 ops/s` | `-3.21%` | `warning` |
| `segment-index-mixed-drain:partitionedIngestMixed:putWorkload` | `5789.232 ops/s` | `5854.997 ops/s` | `+1.14%` | `neutral` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed` | `-` | `212627.282 ops/s` | `-` | `new` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:getWorkload` | `-` | `209988.055 ops/s` | `-` | `new` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:putWorkload` | `-` | `2639.227 ops/s` | `-` | `new` |
