# Benchmark Comparison

- Profile: `segment-index-pr-smoke`
- Baseline SHA: `67d6f015595d472d71dbaf369987d5f6eb97b5b5`
- Candidate SHA: `4ff4dc5cc32614fb8ab0585ca119f71f1530e666`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `segment-index-get-live:getHitSync` | `2434625.181 ops/s` | `2108670.233 ops/s` | `-13.39%` | `worse` |
| `segment-index-get-live:getMissSync` | `4235514.095 ops/s` | `3495166.397 ops/s` | `-17.48%` | `worse` |
| `segment-index-get-multisegment-hot:getHitSync` | `7489.633 ops/s` | `7499.744 ops/s` | `+0.14%` | `neutral` |
| `segment-index-get-multisegment-hot:getMissSync` | `3814528.118 ops/s` | `3394501.870 ops/s` | `-11.01%` | `worse` |
| `segment-index-get-persisted:getHitSync` | `119572.179 ops/s` | `107179.823 ops/s` | `-10.36%` | `worse` |
| `segment-index-get-persisted:getMissSync` | `3751402.345 ops/s` | `3398248.238 ops/s` | `-9.41%` | `worse` |
| `segment-index-hot-route-put:putHotRoute` | `2010777.929 ops/s` | `2053143.108 ops/s` | `+2.11%` | `neutral` |
| `segment-index-hot-route-put:putThenGetHotRoute` | `1033955.055 ops/s` | `1054687.350 ops/s` | `+2.01%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed` | `283007.487 ops/s` | `322732.723 ops/s` | `+14.04%` | `better` |
| `segment-index-mixed-drain:partitionedIngestMixed:getWorkload` | `137276.471 ops/s` | `165358.946 ops/s` | `+20.46%` | `better` |
| `segment-index-mixed-drain:partitionedIngestMixed:putWorkload` | `145731.016 ops/s` | `157373.777 ops/s` | `+7.99%` | `better` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed` | `41280.902 ops/s` | `42490.664 ops/s` | `+2.93%` | `neutral` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:getWorkload` | `36114.855 ops/s` | `37231.873 ops/s` | `+3.09%` | `better` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:putWorkload` | `5166.048 ops/s` | `5258.791 ops/s` | `+1.80%` | `neutral` |
| `segment-index-persisted-mutation:deleteSync` | `2459.668 ops/s` | `3391.063 ops/s` | `+37.87%` | `better` |
| `segment-index-persisted-mutation:putSync` | `440.510 ops/s` | `448.419 ops/s` | `+1.80%` | `neutral` |
