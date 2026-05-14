# Benchmark Comparison

- Profile: `segment-index-pr-smoke`
- Baseline SHA: `e942e650381bf00db7c2fbb3790ab0c49b708f39`
- Candidate SHA: `87792c9594d5ac7f1921c010a1f48d32dd40f19f`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `segment-index-get-live:getHitSync` | `2184440.552 ops/s` | `2251402.834 ops/s` | `+3.07%` | `better` |
| `segment-index-get-live:getMissSync` | `3552489.192 ops/s` | `3348330.836 ops/s` | `-5.75%` | `warning` |
| `segment-index-get-multisegment-hot:getHitSync` | `7569.648 ops/s` | `7650.322 ops/s` | `+1.07%` | `neutral` |
| `segment-index-get-multisegment-hot:getMissSync` | `3650491.279 ops/s` | `3420444.586 ops/s` | `-6.30%` | `warning` |
| `segment-index-get-persisted:getHitSync` | `1983028.673 ops/s` | `1724021.851 ops/s` | `-13.06%` | `worse` |
| `segment-index-get-persisted:getMissSync` | `3625320.125 ops/s` | `3573915.577 ops/s` | `-1.42%` | `neutral` |
| `segment-index-hot-route-put:putHotRoute` | `1914015.937 ops/s` | `1842291.924 ops/s` | `-3.75%` | `warning` |
| `segment-index-hot-route-put:putThenGetHotRoute` | `1007187.466 ops/s` | `1029275.931 ops/s` | `+2.19%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed` | `271174.410 ops/s` | `262603.727 ops/s` | `-3.16%` | `warning` |
| `segment-index-mixed-drain:partitionedIngestMixed:getWorkload` | `118823.228 ops/s` | `97488.819 ops/s` | `-17.95%` | `worse` |
| `segment-index-mixed-drain:partitionedIngestMixed:putWorkload` | `152351.183 ops/s` | `165114.908 ops/s` | `+8.38%` | `better` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed` | `43234.063 ops/s` | `47086.979 ops/s` | `+8.91%` | `better` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:getWorkload` | `38002.816 ops/s` | `41803.360 ops/s` | `+10.00%` | `better` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:putWorkload` | `5231.248 ops/s` | `5283.619 ops/s` | `+1.00%` | `neutral` |
