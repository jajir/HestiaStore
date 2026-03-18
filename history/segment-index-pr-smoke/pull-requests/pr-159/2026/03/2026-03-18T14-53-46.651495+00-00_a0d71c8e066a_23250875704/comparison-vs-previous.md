# Benchmark Comparison

- Profile: `segment-index-pr-smoke`
- Baseline SHA: `52b04525c10e1f18faddd3a3d7f018427f1a5b53`
- Candidate SHA: `a0d71c8e066a382acb9c45d92153ad8eba42f352`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `segment-index-get-multisegment-hot:getHitAsyncJoin` | `-` | `101.798 ops/s` | `-` | `new` |
| `segment-index-get-multisegment-hot:getHitSync` | `-` | `99.256 ops/s` | `-` | `new` |
| `segment-index-get-multisegment-hot:getMissAsyncJoin` | `-` | `239817.343 ops/s` | `-` | `new` |
| `segment-index-get-multisegment-hot:getMissSync` | `-` | `4696312.814 ops/s` | `-` | `new` |
| `segment-index-get-overlay:getHitAsyncJoin` | `160151.872 ops/s` | `235959.871 ops/s` | `+47.34%` | `better` |
| `segment-index-get-overlay:getHitSync` | `4729456.975 ops/s` | `3649408.675 ops/s` | `-22.84%` | `worse` |
| `segment-index-get-overlay:getMissAsyncJoin` | `-` | `235017.448 ops/s` | `-` | `new` |
| `segment-index-get-overlay:getMissSync` | `-` | `4783962.883 ops/s` | `-` | `new` |
| `segment-index-get-persisted:getHitAsyncJoin` | `54557.081 ops/s` | `72336.553 ops/s` | `+32.59%` | `better` |
| `segment-index-get-persisted:getHitSync` | `103784.125 ops/s` | `102803.057 ops/s` | `-0.95%` | `neutral` |
| `segment-index-get-persisted:getMissAsyncJoin` | `-` | `238489.594 ops/s` | `-` | `new` |
| `segment-index-get-persisted:getMissSync` | `-` | `4789659.256 ops/s` | `-` | `new` |
| `segment-index-mixed-drain:partitionedIngestMixed` | `448575.461 ops/s` | `398748.388 ops/s` | `-11.11%` | `worse` |
| `segment-index-mixed-drain:partitionedIngestMixed:getWorkload` | `442786.229 ops/s` | `392914.072 ops/s` | `-11.26%` | `worse` |
| `segment-index-mixed-drain:partitionedIngestMixed:putWorkload` | `5789.232 ops/s` | `5834.315 ops/s` | `+0.78%` | `neutral` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed` | `-` | `204463.360 ops/s` | `-` | `new` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:getWorkload` | `-` | `201682.399 ops/s` | `-` | `new` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:putWorkload` | `-` | `2780.962 ops/s` | `-` | `new` |
