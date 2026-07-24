# Benchmark Comparison

- Profile: `segment-index-nightly`
- Baseline SHA: `a63e8857313f97e6163f9e4567b7002fa0a469ea`
- Candidate SHA: `96593cd49b374380016362f1fc4d5bacdf709832`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `segment-index-get-live:getHitSync` | `2161065.072 ops/s` | `2232987.862 ops/s` | `+3.33%` | `better` |
| `segment-index-get-live:getMissSync` | `2675183.495 ops/s` | `1944126.269 ops/s` | `-27.33%` | `worse` |
| `segment-index-get-persisted:getHitSync` | `1596571.405 ops/s` | `2049901.526 ops/s` | `+28.39%` | `better` |
| `segment-index-get-persisted:getMissSync` | `2563850.004 ops/s` | `1780467.999 ops/s` | `-30.55%` | `worse` |
| `segment-index-hot-route-put:putHotRoute` | `2177717.494 ops/s` | `2069494.809 ops/s` | `-4.97%` | `warning` |
| `segment-index-hot-route-put:putThenGetHotRoute` | `1030309.425 ops/s` | `1117630.536 ops/s` | `+8.48%` | `better` |
| `segment-index-lifecycle:openAndCheckAndRepairConsistency` | `257.286 ms/op` | `258.513 ms/op` | `+0.48%` | `neutral` |
| `segment-index-lifecycle:openAndCompact` | `279.596 ms/op` | `281.542 ms/op` | `+0.70%` | `neutral` |
| `segment-index-lifecycle:openExisting` | `256.135 ms/op` | `255.628 ms/op` | `-0.20%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed` | `441760.203 ops/s` | `437333.175 ops/s` | `-1.00%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed:getWorkload` | `204830.829 ops/s` | `194200.799 ops/s` | `-5.19%` | `warning` |
| `segment-index-mixed-drain:partitionedIngestMixed:putWorkload` | `236929.374 ops/s` | `243132.376 ops/s` | `+2.62%` | `neutral` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed` | `881318.411 ops/s` | `882264.478 ops/s` | `+0.11%` | `neutral` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:getWorkload` | `864269.258 ops/s` | `864241.287 ops/s` | `-0.00%` | `neutral` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:putWorkload` | `17049.153 ops/s` | `18023.191 ops/s` | `+5.71%` | `better` |
