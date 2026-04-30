# Benchmark Comparison

- Profile: `segment-index-pr-smoke`
- Baseline SHA: `67d6f015595d472d71dbaf369987d5f6eb97b5b5`
- Candidate SHA: `a974b97c049086030d67fdff8a26871facd1b900`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `segment-index-get-live:getHitSync` | `2434625.181 ops/s` | `1817549.735 ops/s` | `-25.35%` | `worse` |
| `segment-index-get-live:getMissSync` | `4235514.095 ops/s` | `2458076.533 ops/s` | `-41.97%` | `worse` |
| `segment-index-get-multisegment-hot:getHitSync` | `7489.633 ops/s` | `13382.539 ops/s` | `+78.68%` | `better` |
| `segment-index-get-multisegment-hot:getMissSync` | `3814528.118 ops/s` | `2426854.864 ops/s` | `-36.38%` | `worse` |
| `segment-index-get-persisted:getHitSync` | `119572.179 ops/s` | `107702.059 ops/s` | `-9.93%` | `worse` |
| `segment-index-get-persisted:getMissSync` | `3751402.345 ops/s` | `2541860.789 ops/s` | `-32.24%` | `worse` |
| `segment-index-hot-route-put:putHotRoute` | `2010777.929 ops/s` | `1785891.335 ops/s` | `-11.18%` | `worse` |
| `segment-index-hot-route-put:putThenGetHotRoute` | `1033955.055 ops/s` | `1012627.018 ops/s` | `-2.06%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed` | `283007.487 ops/s` | `241874.776 ops/s` | `-14.53%` | `worse` |
| `segment-index-mixed-drain:partitionedIngestMixed:getWorkload` | `137276.471 ops/s` | `94207.814 ops/s` | `-31.37%` | `worse` |
| `segment-index-mixed-drain:partitionedIngestMixed:putWorkload` | `145731.016 ops/s` | `147666.962 ops/s` | `+1.33%` | `neutral` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed` | `41280.902 ops/s` | `42472.447 ops/s` | `+2.89%` | `neutral` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:getWorkload` | `36114.855 ops/s` | `37211.513 ops/s` | `+3.04%` | `better` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:putWorkload` | `5166.048 ops/s` | `5260.934 ops/s` | `+1.84%` | `neutral` |
| `segment-index-persisted-mutation:deleteSync` | `2459.668 ops/s` | `3541.110 ops/s` | `+43.97%` | `better` |
| `segment-index-persisted-mutation:putSync` | `440.510 ops/s` | `441.404 ops/s` | `+0.20%` | `neutral` |
