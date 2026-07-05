# Benchmark Comparison

- Profile: `segment-index-pr-smoke`
- Baseline SHA: `027ee122b625a6c006b7d04ba7284fb8a4e6d9e8`
- Candidate SHA: `6f0c5c907972c4d7df21da207703a87c21339cd8`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `segment-index-get-live:getHitSync` | `2174587.507 ops/s` | `2354775.626 ops/s` | `+8.29%` | `better` |
| `segment-index-get-live:getMissSync` | `1984911.650 ops/s` | `2379083.459 ops/s` | `+19.86%` | `better` |
| `segment-index-get-persisted:getHitSync` | `1814125.662 ops/s` | `1888080.853 ops/s` | `+4.08%` | `better` |
| `segment-index-get-persisted:getMissSync` | `2220769.690 ops/s` | `2321492.644 ops/s` | `+4.54%` | `better` |
| `segment-index-hot-route-put:putHotRoute` | `2142283.081 ops/s` | `2097758.396 ops/s` | `-2.08%` | `neutral` |
| `segment-index-hot-route-put:putThenGetHotRoute` | `1072641.754 ops/s` | `1086933.572 ops/s` | `+1.33%` | `neutral` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed` | `-` | `613461.171 ops/s` | `-` | `new` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:getWorkload` | `-` | `599067.483 ops/s` | `-` | `new` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:putWorkload` | `-` | `14393.688 ops/s` | `-` | `new` |
