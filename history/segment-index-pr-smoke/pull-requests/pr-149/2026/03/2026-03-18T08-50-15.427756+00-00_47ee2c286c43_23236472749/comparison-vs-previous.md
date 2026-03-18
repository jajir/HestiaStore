# Benchmark Comparison

- Profile: `segment-index-pr-smoke`
- Baseline SHA: `52b04525c10e1f18faddd3a3d7f018427f1a5b53`
- Candidate SHA: `47ee2c286c43f225c37f38a4bab4017092cc27c1`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `segment-index-get-multisegment-hot:getHitAsyncJoin` | `-` | `99.316 ops/s` | `-` | `new` |
| `segment-index-get-multisegment-hot:getHitSync` | `-` | `96.226 ops/s` | `-` | `new` |
| `segment-index-get-multisegment-hot:getMissAsyncJoin` | `-` | `175950.691 ops/s` | `-` | `new` |
| `segment-index-get-multisegment-hot:getMissSync` | `-` | `6846149.015 ops/s` | `-` | `new` |
| `segment-index-get-overlay:getHitAsyncJoin` | `160151.872 ops/s` | `172213.455 ops/s` | `+7.53%` | `better` |
| `segment-index-get-overlay:getHitSync` | `4729456.975 ops/s` | `5433297.620 ops/s` | `+14.88%` | `better` |
| `segment-index-get-overlay:getMissAsyncJoin` | `-` | `178943.272 ops/s` | `-` | `new` |
| `segment-index-get-overlay:getMissSync` | `-` | `7082594.716 ops/s` | `-` | `new` |
| `segment-index-get-persisted:getHitAsyncJoin` | `54557.081 ops/s` | `58497.232 ops/s` | `+7.22%` | `better` |
| `segment-index-get-persisted:getHitSync` | `103784.125 ops/s` | `107423.434 ops/s` | `+3.51%` | `better` |
| `segment-index-get-persisted:getMissAsyncJoin` | `-` | `177326.587 ops/s` | `-` | `new` |
| `segment-index-get-persisted:getMissSync` | `-` | `6629921.799 ops/s` | `-` | `new` |
| `segment-index-mixed-drain:partitionedIngestMixed` | `448575.461 ops/s` | `467441.591 ops/s` | `+4.21%` | `better` |
| `segment-index-mixed-drain:partitionedIngestMixed:getWorkload` | `442786.229 ops/s` | `461632.180 ops/s` | `+4.26%` | `better` |
| `segment-index-mixed-drain:partitionedIngestMixed:putWorkload` | `5789.232 ops/s` | `5809.411 ops/s` | `+0.35%` | `neutral` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed` | `-` | `197800.317 ops/s` | `-` | `new` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:getWorkload` | `-` | `194821.606 ops/s` | `-` | `new` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:putWorkload` | `-` | `2978.711 ops/s` | `-` | `new` |
