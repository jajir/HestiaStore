# Benchmark Comparison

- Profile: `segment-index-nightly`
- Baseline SHA: `a63e8857313f97e6163f9e4567b7002fa0a469ea`
- Candidate SHA: `96593cd49b374380016362f1fc4d5bacdf709832`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `segment-index-get-live:getHitSync` | `2815608.703 ops/s` | `2609233.095 ops/s` | `-7.33%` | `worse` |
| `segment-index-get-live:getMissSync` | `2529384.109 ops/s` | `2448281.812 ops/s` | `-3.21%` | `warning` |
| `segment-index-get-persisted:getHitSync` | `1966579.130 ops/s` | `2122158.408 ops/s` | `+7.91%` | `better` |
| `segment-index-get-persisted:getMissSync` | `2455727.419 ops/s` | `2536122.070 ops/s` | `+3.27%` | `better` |
| `segment-index-hot-route-put:putHotRoute` | `2749531.496 ops/s` | `2829992.062 ops/s` | `+2.93%` | `neutral` |
| `segment-index-hot-route-put:putThenGetHotRoute` | `1363714.305 ops/s` | `1345733.375 ops/s` | `-1.32%` | `neutral` |
| `segment-index-lifecycle:openAndCheckAndRepairConsistency` | `237.674 ms/op` | `236.288 ms/op` | `-0.58%` | `neutral` |
| `segment-index-lifecycle:openAndCompact` | `256.169 ms/op` | `257.445 ms/op` | `+0.50%` | `neutral` |
| `segment-index-lifecycle:openExisting` | `234.179 ms/op` | `236.051 ms/op` | `+0.80%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed` | `534169.046 ops/s` | `530833.740 ops/s` | `-0.62%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed:getWorkload` | `254324.737 ops/s` | `248845.008 ops/s` | `-2.15%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed:putWorkload` | `279844.309 ops/s` | `281988.732 ops/s` | `+0.77%` | `neutral` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed` | `1198074.448 ops/s` | `1225821.621 ops/s` | `+2.32%` | `neutral` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:getWorkload` | `1178415.925 ops/s` | `1206212.958 ops/s` | `+2.36%` | `neutral` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:putWorkload` | `19658.523 ops/s` | `19608.663 ops/s` | `-0.25%` | `neutral` |
