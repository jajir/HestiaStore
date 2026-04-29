# Benchmark Comparison

- Profile: `segment-index-pr-smoke`
- Baseline SHA: `67d6f015595d472d71dbaf369987d5f6eb97b5b5`
- Candidate SHA: `ac3ffe2894137269eff5162164ac256b4e9580f9`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `segment-index-get-live:getHitSync` | `2434625.181 ops/s` | `2372761.568 ops/s` | `-2.54%` | `neutral` |
| `segment-index-get-live:getMissSync` | `4235514.095 ops/s` | `3810954.190 ops/s` | `-10.02%` | `worse` |
| `segment-index-get-multisegment-hot:getHitSync` | `7489.633 ops/s` | `7618.136 ops/s` | `+1.72%` | `neutral` |
| `segment-index-get-multisegment-hot:getMissSync` | `3814528.118 ops/s` | `3838729.884 ops/s` | `+0.63%` | `neutral` |
| `segment-index-get-persisted:getHitSync` | `119572.179 ops/s` | `125679.654 ops/s` | `+5.11%` | `better` |
| `segment-index-get-persisted:getMissSync` | `3751402.345 ops/s` | `3949509.807 ops/s` | `+5.28%` | `better` |
| `segment-index-hot-route-put:putHotRoute` | `2010777.929 ops/s` | `1950942.815 ops/s` | `-2.98%` | `neutral` |
| `segment-index-hot-route-put:putThenGetHotRoute` | `1033955.055 ops/s` | `1062393.451 ops/s` | `+2.75%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed` | `283007.487 ops/s` | `285417.531 ops/s` | `+0.85%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed:getWorkload` | `137276.471 ops/s` | `129412.837 ops/s` | `-5.73%` | `warning` |
| `segment-index-mixed-drain:partitionedIngestMixed:putWorkload` | `145731.016 ops/s` | `156004.694 ops/s` | `+7.05%` | `better` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed` | `41280.902 ops/s` | `35470.634 ops/s` | `-14.07%` | `worse` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:getWorkload` | `36114.855 ops/s` | `30219.723 ops/s` | `-16.32%` | `worse` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:putWorkload` | `5166.048 ops/s` | `5250.911 ops/s` | `+1.64%` | `neutral` |
| `segment-index-persisted-mutation:deleteSync` | `2459.668 ops/s` | `2631.370 ops/s` | `+6.98%` | `better` |
| `segment-index-persisted-mutation:putSync` | `440.510 ops/s` | `451.109 ops/s` | `+2.41%` | `neutral` |
