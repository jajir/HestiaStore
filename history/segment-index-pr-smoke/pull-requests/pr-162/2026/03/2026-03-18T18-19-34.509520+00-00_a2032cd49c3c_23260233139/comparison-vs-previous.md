# Benchmark Comparison

- Profile: `segment-index-pr-smoke`
- Baseline SHA: `52b04525c10e1f18faddd3a3d7f018427f1a5b53`
- Candidate SHA: `a2032cd49c3cf809a7e4588af866ad5645749fe6`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `segment-index-get-multisegment-hot:getHitAsyncJoin` | `-` | `98.067 ops/s` | `-` | `new` |
| `segment-index-get-multisegment-hot:getHitSync` | `-` | `97.399 ops/s` | `-` | `new` |
| `segment-index-get-multisegment-hot:getMissAsyncJoin` | `-` | `165887.164 ops/s` | `-` | `new` |
| `segment-index-get-multisegment-hot:getMissSync` | `-` | `6297951.275 ops/s` | `-` | `new` |
| `segment-index-get-overlay:getHitAsyncJoin` | `160151.872 ops/s` | `161757.530 ops/s` | `+1.00%` | `neutral` |
| `segment-index-get-overlay:getHitSync` | `4729456.975 ops/s` | `5216119.220 ops/s` | `+10.29%` | `better` |
| `segment-index-get-overlay:getMissAsyncJoin` | `-` | `159923.707 ops/s` | `-` | `new` |
| `segment-index-get-overlay:getMissSync` | `-` | `6922920.069 ops/s` | `-` | `new` |
| `segment-index-get-persisted:getHitAsyncJoin` | `54557.081 ops/s` | `58616.038 ops/s` | `+7.44%` | `better` |
| `segment-index-get-persisted:getHitSync` | `103784.125 ops/s` | `104043.620 ops/s` | `+0.25%` | `neutral` |
| `segment-index-get-persisted:getMissAsyncJoin` | `-` | `167529.449 ops/s` | `-` | `new` |
| `segment-index-get-persisted:getMissSync` | `-` | `6609189.562 ops/s` | `-` | `new` |
| `segment-index-mixed-drain:partitionedIngestMixed` | `448575.461 ops/s` | `444675.664 ops/s` | `-0.87%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed:getWorkload` | `442786.229 ops/s` | `438767.132 ops/s` | `-0.91%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed:putWorkload` | `5789.232 ops/s` | `5908.533 ops/s` | `+2.06%` | `neutral` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed` | `-` | `213361.855 ops/s` | `-` | `new` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:getWorkload` | `-` | `210612.893 ops/s` | `-` | `new` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:putWorkload` | `-` | `2748.963 ops/s` | `-` | `new` |
