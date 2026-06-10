# Benchmark Comparison

- Profile: `segment-index-pr-smoke`
- Baseline SHA: `ec66db06e098be9f665626456c1b5db07b2abb7c`
- Candidate SHA: `c01ff7ad616cf77a4389bedba2ae6d54915f3623`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `segment-index-get-live:getHitSync` | `2247402.780 ops/s` | `2325368.063 ops/s` | `+3.47%` | `better` |
| `segment-index-get-live:getMissSync` | `2250763.399 ops/s` | `2138871.780 ops/s` | `-4.97%` | `warning` |
| `segment-index-get-persisted:getHitSync` | `1651939.799 ops/s` | `1828223.732 ops/s` | `+10.67%` | `better` |
| `segment-index-get-persisted:getMissSync` | `2076888.412 ops/s` | `2192759.744 ops/s` | `+5.58%` | `better` |
| `segment-index-hot-route-put:putHotRoute` | `2055339.947 ops/s` | `2143811.494 ops/s` | `+4.30%` | `better` |
| `segment-index-hot-route-put:putThenGetHotRoute` | `1155305.337 ops/s` | `1162720.690 ops/s` | `+0.64%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed` | `307850.443 ops/s` | `274481.253 ops/s` | `-10.84%` | `worse` |
| `segment-index-mixed-drain:partitionedIngestMixed:getWorkload` | `159080.524 ops/s` | `155630.658 ops/s` | `-2.17%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed:putWorkload` | `148769.919 ops/s` | `118850.595 ops/s` | `-20.11%` | `worse` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed` | `178461.852 ops/s` | `181118.823 ops/s` | `+1.49%` | `neutral` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:getWorkload` | `164337.518 ops/s` | `166889.735 ops/s` | `+1.55%` | `neutral` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:putWorkload` | `14124.334 ops/s` | `14229.087 ops/s` | `+0.74%` | `neutral` |
