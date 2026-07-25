# Benchmark Comparison

- Profile: `segment-index-nightly`
- Baseline SHA: `a63e8857313f97e6163f9e4567b7002fa0a469ea`
- Candidate SHA: `96593cd49b374380016362f1fc4d5bacdf709832`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `segment-index-get-live:getHitSync` | `2076738.601 ops/s` | `2242063.628 ops/s` | `+7.96%` | `better` |
| `segment-index-get-live:getMissSync` | `2668347.321 ops/s` | `1996464.472 ops/s` | `-25.18%` | `worse` |
| `segment-index-get-persisted:getHitSync` | `1565518.583 ops/s` | `1735914.478 ops/s` | `+10.88%` | `better` |
| `segment-index-get-persisted:getMissSync` | `2121184.360 ops/s` | `2085850.038 ops/s` | `-1.67%` | `neutral` |
| `segment-index-hot-route-put:putHotRoute` | `2038963.986 ops/s` | `2141304.796 ops/s` | `+5.02%` | `better` |
| `segment-index-hot-route-put:putThenGetHotRoute` | `1060802.945 ops/s` | `1085169.550 ops/s` | `+2.30%` | `neutral` |
| `segment-index-lifecycle:openAndCheckAndRepairConsistency` | `252.329 ms/op` | `254.839 ms/op` | `+0.99%` | `neutral` |
| `segment-index-lifecycle:openAndCompact` | `276.286 ms/op` | `277.992 ms/op` | `+0.62%` | `neutral` |
| `segment-index-lifecycle:openExisting` | `250.117 ms/op` | `252.389 ms/op` | `+0.91%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed` | `447434.021 ops/s` | `438738.944 ops/s` | `-1.94%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed:getWorkload` | `206263.594 ops/s` | `202504.527 ops/s` | `-1.82%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed:putWorkload` | `241170.427 ops/s` | `236234.417 ops/s` | `-2.05%` | `neutral` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed` | `880989.730 ops/s` | `865076.269 ops/s` | `-1.81%` | `neutral` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:getWorkload` | `863727.122 ops/s` | `847610.760 ops/s` | `-1.87%` | `neutral` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:putWorkload` | `17262.609 ops/s` | `17465.509 ops/s` | `+1.18%` | `neutral` |
