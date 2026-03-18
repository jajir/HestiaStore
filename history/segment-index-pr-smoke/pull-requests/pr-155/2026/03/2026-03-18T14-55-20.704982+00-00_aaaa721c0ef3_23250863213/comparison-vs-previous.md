# Benchmark Comparison

- Profile: `segment-index-pr-smoke`
- Baseline SHA: `52b04525c10e1f18faddd3a3d7f018427f1a5b53`
- Candidate SHA: `aaaa721c0ef3c1f4e99925ff593ad5338089a44b`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `segment-index-get-multisegment-hot:getHitAsyncJoin` | `-` | `101.791 ops/s` | `-` | `new` |
| `segment-index-get-multisegment-hot:getHitSync` | `-` | `89.908 ops/s` | `-` | `new` |
| `segment-index-get-multisegment-hot:getMissAsyncJoin` | `-` | `239929.421 ops/s` | `-` | `new` |
| `segment-index-get-multisegment-hot:getMissSync` | `-` | `4632973.372 ops/s` | `-` | `new` |
| `segment-index-get-overlay:getHitAsyncJoin` | `160151.872 ops/s` | `230891.948 ops/s` | `+44.17%` | `better` |
| `segment-index-get-overlay:getHitSync` | `4729456.975 ops/s` | `3741650.396 ops/s` | `-20.89%` | `worse` |
| `segment-index-get-overlay:getMissAsyncJoin` | `-` | `224038.829 ops/s` | `-` | `new` |
| `segment-index-get-overlay:getMissSync` | `-` | `4910138.922 ops/s` | `-` | `new` |
| `segment-index-get-persisted:getHitAsyncJoin` | `54557.081 ops/s` | `74446.031 ops/s` | `+36.46%` | `better` |
| `segment-index-get-persisted:getHitSync` | `103784.125 ops/s` | `101731.271 ops/s` | `-1.98%` | `neutral` |
| `segment-index-get-persisted:getMissAsyncJoin` | `-` | `231976.717 ops/s` | `-` | `new` |
| `segment-index-get-persisted:getMissSync` | `-` | `4807271.549 ops/s` | `-` | `new` |
| `segment-index-mixed-drain:partitionedIngestMixed` | `448575.461 ops/s` | `422406.284 ops/s` | `-5.83%` | `warning` |
| `segment-index-mixed-drain:partitionedIngestMixed:getWorkload` | `442786.229 ops/s` | `416684.024 ops/s` | `-5.89%` | `warning` |
| `segment-index-mixed-drain:partitionedIngestMixed:putWorkload` | `5789.232 ops/s` | `5722.260 ops/s` | `-1.16%` | `neutral` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed` | `-` | `180645.332 ops/s` | `-` | `new` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:getWorkload` | `-` | `177966.356 ops/s` | `-` | `new` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:putWorkload` | `-` | `2678.975 ops/s` | `-` | `new` |
