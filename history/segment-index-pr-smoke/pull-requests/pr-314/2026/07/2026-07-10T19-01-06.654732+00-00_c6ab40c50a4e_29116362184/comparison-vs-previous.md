# Benchmark Comparison

- Profile: `segment-index-pr-smoke`
- Baseline SHA: `c4e36341d5090cbedaf4272dfe8a9d7ca1569f19`
- Candidate SHA: `c6ab40c50a4ea68dc949abe447cad0a6aceb0c64`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `segment-index-get-live:getHitSync` | `2334722.313 ops/s` | `2285127.627 ops/s` | `-2.12%` | `neutral` |
| `segment-index-get-live:getMissSync` | `2141101.358 ops/s` | `2325134.112 ops/s` | `+8.60%` | `better` |
| `segment-index-get-persisted:getHitSync` | `1957777.631 ops/s` | `2158366.617 ops/s` | `+10.25%` | `better` |
| `segment-index-get-persisted:getMissSync` | `1985092.750 ops/s` | `2215080.522 ops/s` | `+11.59%` | `better` |
| `segment-index-hot-route-put:putHotRoute` | `2158892.692 ops/s` | `2143303.971 ops/s` | `-0.72%` | `neutral` |
| `segment-index-hot-route-put:putThenGetHotRoute` | `1142378.592 ops/s` | `1040730.046 ops/s` | `-8.90%` | `worse` |
| `segment-index-mixed-drain:partitionedIngestMixed` | `462652.456 ops/s` | `501236.854 ops/s` | `+8.34%` | `better` |
| `segment-index-mixed-drain:partitionedIngestMixed:getWorkload` | `294801.129 ops/s` | `339262.000 ops/s` | `+15.08%` | `better` |
| `segment-index-mixed-drain:partitionedIngestMixed:putWorkload` | `167851.328 ops/s` | `161974.854 ops/s` | `-3.50%` | `warning` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed` | `532433.795 ops/s` | `600600.839 ops/s` | `+12.80%` | `better` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:getWorkload` | `518798.947 ops/s` | `584731.388 ops/s` | `+12.71%` | `better` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:putWorkload` | `13634.847 ops/s` | `15869.451 ops/s` | `+16.39%` | `better` |
