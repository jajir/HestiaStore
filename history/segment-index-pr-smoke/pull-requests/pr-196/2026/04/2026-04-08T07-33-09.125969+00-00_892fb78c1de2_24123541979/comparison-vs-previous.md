# Benchmark Comparison

- Bootstrap mode: no prior canonical baseline exists yet, and fallback git baseline predates the benchmark harness.
- Candidate results below are the first canonical benchmark snapshot for this workflow.


- Profile: `segment-index-pr-smoke`
- Baseline SHA: `892fb78c1de2872c670e9929ca37a5f14ad63fec`
- Candidate SHA: `892fb78c1de2872c670e9929ca37a5f14ad63fec`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `segment-index-get-live:getHitAsyncJoin` | `171868.411 ops/s` | `171868.411 ops/s` | `+0.00%` | `neutral` |
| `segment-index-get-live:getHitSync` | `3613867.328 ops/s` | `3613867.328 ops/s` | `+0.00%` | `neutral` |
| `segment-index-get-live:getMissAsyncJoin` | `174464.813 ops/s` | `174464.813 ops/s` | `+0.00%` | `neutral` |
| `segment-index-get-live:getMissSync` | `4091931.436 ops/s` | `4091931.436 ops/s` | `+0.00%` | `neutral` |
| `segment-index-get-multisegment-hot:getHitAsyncJoin` | `95.227 ops/s` | `95.227 ops/s` | `+0.00%` | `neutral` |
| `segment-index-get-multisegment-hot:getHitSync` | `83.966 ops/s` | `83.966 ops/s` | `+0.00%` | `neutral` |
| `segment-index-get-multisegment-hot:getMissAsyncJoin` | `185252.655 ops/s` | `185252.655 ops/s` | `+0.00%` | `neutral` |
| `segment-index-get-multisegment-hot:getMissSync` | `3791299.307 ops/s` | `3791299.307 ops/s` | `+0.00%` | `neutral` |
| `segment-index-get-persisted:getHitAsyncJoin` | `69383.709 ops/s` | `69383.709 ops/s` | `+0.00%` | `neutral` |
| `segment-index-get-persisted:getHitSync` | `127639.459 ops/s` | `127639.459 ops/s` | `+0.00%` | `neutral` |
| `segment-index-get-persisted:getMissAsyncJoin` | `162530.677 ops/s` | `162530.677 ops/s` | `+0.00%` | `neutral` |
| `segment-index-get-persisted:getMissSync` | `3969561.949 ops/s` | `3969561.949 ops/s` | `+0.00%` | `neutral` |
| `segment-index-hot-route-put:putHotRoute` | `2556204.291 ops/s` | `2556204.291 ops/s` | `+0.00%` | `neutral` |
| `segment-index-hot-route-put:putThenGetHotRoute` | `1470680.346 ops/s` | `1470680.346 ops/s` | `+0.00%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed` | `334886.045 ops/s` | `334886.045 ops/s` | `+0.00%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed:getWorkload` | `148841.721 ops/s` | `148841.721 ops/s` | `+0.00%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed:putWorkload` | `186044.324 ops/s` | `186044.324 ops/s` | `+0.00%` | `neutral` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed` | `367604.361 ops/s` | `367604.361 ops/s` | `+0.00%` | `neutral` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:getWorkload` | `38404.880 ops/s` | `38404.880 ops/s` | `+0.00%` | `neutral` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:putWorkload` | `329199.481 ops/s` | `329199.481 ops/s` | `+0.00%` | `neutral` |
| `segment-index-persisted-mutation:deleteAsyncJoin` | `2142.987 ops/s` | `2142.987 ops/s` | `+0.00%` | `neutral` |
| `segment-index-persisted-mutation:deleteSync` | `2470.446 ops/s` | `2470.446 ops/s` | `+0.00%` | `neutral` |
| `segment-index-persisted-mutation:putAsyncJoin` | `2086.487 ops/s` | `2086.487 ops/s` | `+0.00%` | `neutral` |
| `segment-index-persisted-mutation:putSync` | `2419.920 ops/s` | `2419.920 ops/s` | `+0.00%` | `neutral` |
