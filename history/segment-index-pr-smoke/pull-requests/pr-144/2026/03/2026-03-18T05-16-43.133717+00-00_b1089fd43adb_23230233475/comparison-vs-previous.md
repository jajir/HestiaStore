# Benchmark Comparison

- Profile: `segment-index-pr-smoke`
- Baseline SHA: `52b04525c10e1f18faddd3a3d7f018427f1a5b53`
- Candidate SHA: `b1089fd43adbdb932bb9a5ac0b1d448467cb4db9`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `segment-index-get-multisegment-hot:getHitAsyncJoin` | `-` | `94.786 ops/s` | `-` | `new` |
| `segment-index-get-multisegment-hot:getHitSync` | `-` | `100.001 ops/s` | `-` | `new` |
| `segment-index-get-multisegment-hot:getMissAsyncJoin` | `-` | `171891.133 ops/s` | `-` | `new` |
| `segment-index-get-multisegment-hot:getMissSync` | `-` | `6460097.262 ops/s` | `-` | `new` |
| `segment-index-get-overlay:getHitAsyncJoin` | `160151.872 ops/s` | `161297.963 ops/s` | `+0.72%` | `neutral` |
| `segment-index-get-overlay:getHitSync` | `4729456.975 ops/s` | `5313255.986 ops/s` | `+12.34%` | `better` |
| `segment-index-get-overlay:getMissAsyncJoin` | `-` | `170017.372 ops/s` | `-` | `new` |
| `segment-index-get-overlay:getMissSync` | `-` | `6534235.460 ops/s` | `-` | `new` |
| `segment-index-get-persisted:getHitAsyncJoin` | `54557.081 ops/s` | `56891.619 ops/s` | `+4.28%` | `better` |
| `segment-index-get-persisted:getHitSync` | `103784.125 ops/s` | `106315.303 ops/s` | `+2.44%` | `neutral` |
| `segment-index-get-persisted:getMissAsyncJoin` | `-` | `171290.861 ops/s` | `-` | `new` |
| `segment-index-get-persisted:getMissSync` | `-` | `7004161.950 ops/s` | `-` | `new` |
| `segment-index-mixed-drain:partitionedIngestMixed` | `448575.461 ops/s` | `440670.883 ops/s` | `-1.76%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed:getWorkload` | `442786.229 ops/s` | `434977.247 ops/s` | `-1.76%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed:putWorkload` | `5789.232 ops/s` | `5693.636 ops/s` | `-1.65%` | `neutral` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed` | `-` | `184189.460 ops/s` | `-` | `new` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:getWorkload` | `-` | `181116.212 ops/s` | `-` | `new` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:putWorkload` | `-` | `3073.248 ops/s` | `-` | `new` |
