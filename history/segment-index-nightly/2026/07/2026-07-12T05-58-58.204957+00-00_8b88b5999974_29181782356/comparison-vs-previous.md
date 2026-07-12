# Benchmark Comparison

- Profile: `segment-index-nightly`
- Baseline SHA: `1e37c5925410c7e6649b1fa1d4fd33325bcb2d4c`
- Candidate SHA: `8b88b599997497df9b956d4be577a2b7424cedcb`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `segment-index-get-live:getHitSync` | `2089288.479 ops/s` | `2125645.358 ops/s` | `+1.74%` | `neutral` |
| `segment-index-get-live:getMissSync` | `1888979.235 ops/s` | `2017135.529 ops/s` | `+6.78%` | `better` |
| `segment-index-get-persisted:getHitSync` | `1661265.382 ops/s` | `1912133.898 ops/s` | `+15.10%` | `better` |
| `segment-index-get-persisted:getMissSync` | `1962251.639 ops/s` | `1827678.148 ops/s` | `-6.86%` | `warning` |
| `segment-index-hot-route-put:putHotRoute` | `2137911.503 ops/s` | `2144316.824 ops/s` | `+0.30%` | `neutral` |
| `segment-index-hot-route-put:putThenGetHotRoute` | `1032813.635 ops/s` | `1073106.609 ops/s` | `+3.90%` | `better` |
| `segment-index-lifecycle:openAndCheckAndRepairConsistency` | `261.477 ms/op` | `261.413 ms/op` | `-0.02%` | `neutral` |
| `segment-index-lifecycle:openAndCompact` | `282.598 ms/op` | `283.557 ms/op` | `+0.34%` | `neutral` |
| `segment-index-lifecycle:openExisting` | `256.290 ms/op` | `257.157 ms/op` | `+0.34%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed` | `425208.526 ops/s` | `414178.817 ops/s` | `-2.59%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed:getWorkload` | `201288.872 ops/s` | `187318.679 ops/s` | `-6.94%` | `warning` |
| `segment-index-mixed-drain:partitionedIngestMixed:putWorkload` | `223919.654 ops/s` | `226860.138 ops/s` | `+1.31%` | `neutral` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed` | `830032.172 ops/s` | `848006.242 ops/s` | `+2.17%` | `neutral` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:getWorkload` | `813609.973 ops/s` | `830659.679 ops/s` | `+2.10%` | `neutral` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:putWorkload` | `16422.199 ops/s` | `17346.563 ops/s` | `+5.63%` | `better` |
