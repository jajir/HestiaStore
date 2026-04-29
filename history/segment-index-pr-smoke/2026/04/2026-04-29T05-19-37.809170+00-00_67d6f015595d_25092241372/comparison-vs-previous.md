# Benchmark Comparison

- Profile: `segment-index-pr-smoke`
- Baseline SHA: `d071c9602c6022fca98dfbd1e869ca129e4cd557`
- Candidate SHA: `67d6f015595d472d71dbaf369987d5f6eb97b5b5`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `segment-index-get-live:getHitSync` | `2146251.189 ops/s` | `2434625.181 ops/s` | `+13.44%` | `better` |
| `segment-index-get-live:getMissSync` | `3623888.505 ops/s` | `4235514.095 ops/s` | `+16.88%` | `better` |
| `segment-index-get-multisegment-hot:getHitSync` | `7590.125 ops/s` | `7489.633 ops/s` | `-1.32%` | `neutral` |
| `segment-index-get-multisegment-hot:getMissSync` | `3557139.759 ops/s` | `3814528.118 ops/s` | `+7.24%` | `better` |
| `segment-index-get-persisted:getHitSync` | `121505.264 ops/s` | `119572.179 ops/s` | `-1.59%` | `neutral` |
| `segment-index-get-persisted:getMissSync` | `3532808.625 ops/s` | `3751402.345 ops/s` | `+6.19%` | `better` |
| `segment-index-hot-route-put:putHotRoute` | `1965836.839 ops/s` | `2010777.929 ops/s` | `+2.29%` | `neutral` |
| `segment-index-hot-route-put:putThenGetHotRoute` | `1089192.007 ops/s` | `1033955.055 ops/s` | `-5.07%` | `warning` |
| `segment-index-mixed-drain:partitionedIngestMixed` | `285913.041 ops/s` | `283007.487 ops/s` | `-1.02%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed:getWorkload` | `107818.573 ops/s` | `137276.471 ops/s` | `+27.32%` | `better` |
| `segment-index-mixed-drain:partitionedIngestMixed:putWorkload` | `178094.468 ops/s` | `145731.016 ops/s` | `-18.17%` | `worse` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed` | `49402.235 ops/s` | `41280.902 ops/s` | `-16.44%` | `worse` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:getWorkload` | `44096.518 ops/s` | `36114.855 ops/s` | `-18.10%` | `worse` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:putWorkload` | `5305.717 ops/s` | `5166.048 ops/s` | `-2.63%` | `neutral` |
| `segment-index-persisted-mutation:deleteSync` | `3460.051 ops/s` | `2459.668 ops/s` | `-28.91%` | `worse` |
| `segment-index-persisted-mutation:putSync` | `457.164 ops/s` | `440.510 ops/s` | `-3.64%` | `warning` |
