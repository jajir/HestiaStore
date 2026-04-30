# Benchmark Comparison

- Profile: `segment-index-pr-smoke`
- Baseline SHA: `67d6f015595d472d71dbaf369987d5f6eb97b5b5`
- Candidate SHA: `cb2ffb93fb0e8b622368d230177121667293a9d7`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `segment-index-get-live:getHitSync` | `2434625.181 ops/s` | `1851218.625 ops/s` | `-23.96%` | `worse` |
| `segment-index-get-live:getMissSync` | `4235514.095 ops/s` | `2622533.567 ops/s` | `-38.08%` | `worse` |
| `segment-index-get-multisegment-hot:getHitSync` | `7489.633 ops/s` | `13153.626 ops/s` | `+75.62%` | `better` |
| `segment-index-get-multisegment-hot:getMissSync` | `3814528.118 ops/s` | `2492013.057 ops/s` | `-34.67%` | `worse` |
| `segment-index-get-persisted:getHitSync` | `119572.179 ops/s` | `118963.380 ops/s` | `-0.51%` | `neutral` |
| `segment-index-get-persisted:getMissSync` | `3751402.345 ops/s` | `2550320.254 ops/s` | `-32.02%` | `worse` |
| `segment-index-hot-route-put:putHotRoute` | `2010777.929 ops/s` | `1801147.552 ops/s` | `-10.43%` | `worse` |
| `segment-index-hot-route-put:putThenGetHotRoute` | `1033955.055 ops/s` | `1043473.104 ops/s` | `+0.92%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed` | `283007.487 ops/s` | `251908.324 ops/s` | `-10.99%` | `worse` |
| `segment-index-mixed-drain:partitionedIngestMixed:getWorkload` | `137276.471 ops/s` | `91375.900 ops/s` | `-33.44%` | `worse` |
| `segment-index-mixed-drain:partitionedIngestMixed:putWorkload` | `145731.016 ops/s` | `160532.424 ops/s` | `+10.16%` | `better` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed` | `41280.902 ops/s` | `47596.284 ops/s` | `+15.30%` | `better` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:getWorkload` | `36114.855 ops/s` | `42310.020 ops/s` | `+17.15%` | `better` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:putWorkload` | `5166.048 ops/s` | `5286.264 ops/s` | `+2.33%` | `neutral` |
| `segment-index-persisted-mutation:deleteSync` | `2459.668 ops/s` | `3548.605 ops/s` | `+44.27%` | `better` |
| `segment-index-persisted-mutation:putSync` | `440.510 ops/s` | `466.022 ops/s` | `+5.79%` | `better` |
