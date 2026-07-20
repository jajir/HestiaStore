# Benchmark Comparison

- Profile: `segment-index-nightly`
- Baseline SHA: `21d1da156bc01e2f39b9f119c5882d79b3da438a`
- Candidate SHA: `dce1fbc911dbad09d8df685697a7c8b43069ed09`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `segment-index-get-live:getHitSync` | `2108666.251 ops/s` | `2093278.893 ops/s` | `-0.73%` | `neutral` |
| `segment-index-get-live:getMissSync` | `1855830.303 ops/s` | `1966690.532 ops/s` | `+5.97%` | `better` |
| `segment-index-get-persisted:getHitSync` | `1724603.590 ops/s` | `1476193.423 ops/s` | `-14.40%` | `worse` |
| `segment-index-get-persisted:getMissSync` | `2155060.742 ops/s` | `1945542.212 ops/s` | `-9.72%` | `worse` |
| `segment-index-hot-route-put:putHotRoute` | `2038278.973 ops/s` | `1973356.652 ops/s` | `-3.19%` | `warning` |
| `segment-index-hot-route-put:putThenGetHotRoute` | `1001259.070 ops/s` | `1014677.154 ops/s` | `+1.34%` | `neutral` |
| `segment-index-lifecycle:openAndCheckAndRepairConsistency` | `251.244 ms/op` | `252.847 ms/op` | `+0.64%` | `neutral` |
| `segment-index-lifecycle:openAndCompact` | `274.549 ms/op` | `274.884 ms/op` | `+0.12%` | `neutral` |
| `segment-index-lifecycle:openExisting` | `250.685 ms/op` | `247.782 ms/op` | `-1.16%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed` | `438093.092 ops/s` | `450976.860 ops/s` | `+2.94%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed:getWorkload` | `195697.400 ops/s` | `214396.804 ops/s` | `+9.56%` | `better` |
| `segment-index-mixed-drain:partitionedIngestMixed:putWorkload` | `242395.692 ops/s` | `236580.057 ops/s` | `-2.40%` | `neutral` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed` | `839227.952 ops/s` | `889760.673 ops/s` | `+6.02%` | `better` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:getWorkload` | `822077.843 ops/s` | `873139.731 ops/s` | `+6.21%` | `better` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:putWorkload` | `17150.110 ops/s` | `16620.942 ops/s` | `-3.09%` | `warning` |
