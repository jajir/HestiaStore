# Benchmark Comparison

- Profile: `segment-index-nightly`
- Baseline SHA: `a63e8857313f97e6163f9e4567b7002fa0a469ea`
- Candidate SHA: `96593cd49b374380016362f1fc4d5bacdf709832`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `segment-index-get-live:getHitSync` | `1960259.431 ops/s` | `2161386.478 ops/s` | `+10.26%` | `better` |
| `segment-index-get-live:getMissSync` | `1944446.514 ops/s` | `2011089.172 ops/s` | `+3.43%` | `better` |
| `segment-index-get-persisted:getHitSync` | `1772153.436 ops/s` | `1462405.752 ops/s` | `-17.48%` | `worse` |
| `segment-index-get-persisted:getMissSync` | `1869647.868 ops/s` | `1883736.874 ops/s` | `+0.75%` | `neutral` |
| `segment-index-hot-route-put:putHotRoute` | `2116186.499 ops/s` | `2131959.571 ops/s` | `+0.75%` | `neutral` |
| `segment-index-hot-route-put:putThenGetHotRoute` | `1108384.613 ops/s` | `1126345.445 ops/s` | `+1.62%` | `neutral` |
| `segment-index-lifecycle:openAndCheckAndRepairConsistency` | `260.353 ms/op` | `260.217 ms/op` | `-0.05%` | `neutral` |
| `segment-index-lifecycle:openAndCompact` | `281.003 ms/op` | `283.434 ms/op` | `+0.87%` | `neutral` |
| `segment-index-lifecycle:openExisting` | `255.064 ms/op` | `255.129 ms/op` | `+0.03%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed` | `430078.125 ops/s` | `434334.576 ops/s` | `+0.99%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed:getWorkload` | `191876.130 ops/s` | `195110.794 ops/s` | `+1.69%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed:putWorkload` | `238201.995 ops/s` | `239223.782 ops/s` | `+0.43%` | `neutral` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed` | `859816.742 ops/s` | `892307.895 ops/s` | `+3.78%` | `better` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:getWorkload` | `843116.387 ops/s` | `874102.703 ops/s` | `+3.68%` | `better` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:putWorkload` | `16700.355 ops/s` | `18205.192 ops/s` | `+9.01%` | `better` |
