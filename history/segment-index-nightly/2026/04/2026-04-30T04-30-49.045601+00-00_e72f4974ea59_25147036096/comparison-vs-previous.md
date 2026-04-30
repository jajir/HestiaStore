# Benchmark Comparison

- Candidate labels rerun 3x total and median-merged because the initial regression exceeded threshold with high relative score error: `segment-index-get-multisegment-cold,segment-index-get-multisegment-hot`


- Profile: `segment-index-nightly`
- Baseline SHA: `a87e6e844c0f83bfc93f8ff3748153d6ffec3dbb`
- Candidate SHA: `e72f4974ea597aa48d2725b131d13e39bcfa6872`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `segment-index-get-multisegment-cold:getHitAsyncJoin` | `49.068 ops/s` | `45.847 ops/s` | `-6.56%` | `warning` |
| `segment-index-get-multisegment-cold:getHitSync` | `53.146 ops/s` | `47.682 ops/s` | `-10.28%` | `worse` |
| `segment-index-get-multisegment-cold:getMissAsyncJoin` | `171357.466 ops/s` | `170896.942 ops/s` | `-0.27%` | `neutral` |
| `segment-index-get-multisegment-cold:getMissSync` | `3992353.208 ops/s` | `3579209.297 ops/s` | `-10.35%` | `worse` |
| `segment-index-get-multisegment-hot:getHitAsyncJoin` | `94.737 ops/s` | `95.536 ops/s` | `+0.84%` | `neutral` |
| `segment-index-get-multisegment-hot:getHitSync` | `102.890 ops/s` | `93.554 ops/s` | `-9.07%` | `worse` |
| `segment-index-get-multisegment-hot:getMissAsyncJoin` | `171117.397 ops/s` | `172196.216 ops/s` | `+0.63%` | `neutral` |
| `segment-index-get-multisegment-hot:getMissSync` | `3514423.797 ops/s` | `3636512.921 ops/s` | `+3.47%` | `better` |
| `segment-index-get-overlay:getHitAsyncJoin` | `156377.760 ops/s` | `161025.484 ops/s` | `+2.97%` | `neutral` |
| `segment-index-get-overlay:getHitSync` | `3658692.975 ops/s` | `3775546.589 ops/s` | `+3.19%` | `better` |
| `segment-index-get-overlay:getMissAsyncJoin` | `155894.336 ops/s` | `163864.691 ops/s` | `+5.11%` | `better` |
| `segment-index-get-overlay:getMissSync` | `3951604.852 ops/s` | `3768262.983 ops/s` | `-4.64%` | `warning` |
| `segment-index-get-persisted:getHitAsyncJoin` | `62229.374 ops/s` | `62763.848 ops/s` | `+0.86%` | `neutral` |
| `segment-index-get-persisted:getHitSync` | `117081.860 ops/s` | `115807.950 ops/s` | `-1.09%` | `neutral` |
| `segment-index-get-persisted:getMissAsyncJoin` | `160437.621 ops/s` | `166146.586 ops/s` | `+3.56%` | `better` |
| `segment-index-get-persisted:getMissSync` | `3679679.558 ops/s` | `3693468.360 ops/s` | `+0.37%` | `neutral` |
| `segment-index-hot-partition-put:putHotPartition` | `2891908.370 ops/s` | `3141057.705 ops/s` | `+8.62%` | `better` |
| `segment-index-hot-partition-put:putThenGetHotPartition` | `1417065.612 ops/s` | `1641531.231 ops/s` | `+15.84%` | `better` |
| `segment-index-lifecycle:openAndCheckAndRepairConsistency` | `243.837 ms/op` | `241.602 ms/op` | `-0.92%` | `neutral` |
| `segment-index-lifecycle:openAndCompact` | `265.130 ms/op` | `264.745 ms/op` | `-0.14%` | `neutral` |
| `segment-index-lifecycle:openExisting` | `240.721 ms/op` | `243.309 ms/op` | `+1.08%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed` | `521387.042 ops/s` | `551537.200 ops/s` | `+5.78%` | `better` |
| `segment-index-mixed-drain:partitionedIngestMixed:getWorkload` | `515990.404 ops/s` | `546202.672 ops/s` | `+5.86%` | `better` |
| `segment-index-mixed-drain:partitionedIngestMixed:putWorkload` | `5396.638 ops/s` | `5334.528 ops/s` | `-1.15%` | `neutral` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed` | `254956.978 ops/s` | `271333.502 ops/s` | `+6.42%` | `better` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:getWorkload` | `253404.174 ops/s` | `269822.497 ops/s` | `+6.48%` | `better` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:putWorkload` | `1552.803 ops/s` | `1511.006 ops/s` | `-2.69%` | `neutral` |
| `segment-index-persisted-mutation:deleteAsyncJoin` | `2027.055 ops/s` | `2072.729 ops/s` | `+2.25%` | `neutral` |
| `segment-index-persisted-mutation:deleteSync` | `2249.266 ops/s` | `2309.552 ops/s` | `+2.68%` | `neutral` |
| `segment-index-persisted-mutation:putAsyncJoin` | `1994.826 ops/s` | `2037.805 ops/s` | `+2.15%` | `neutral` |
| `segment-index-persisted-mutation:putSync` | `2234.517 ops/s` | `2239.875 ops/s` | `+0.24%` | `neutral` |
| `single-chunk-entry-write-compact:writeEntrySteadyState` | `8585209.731 ops/s` | `8570023.213 ops/s` | `-0.18%` | `neutral` |
| `single-chunk-entry-write-large:writeEntrySteadyState` | `7898319.968 ops/s` | `7976521.724 ops/s` | `+0.99%` | `neutral` |
| `sorted-data-diff-key-read-compact:readNextKey` | `8757871.501 ops/s` | `9221189.282 ops/s` | `+5.29%` | `better` |
| `sorted-data-diff-key-read-large:readNextKey` | `7174234.889 ops/s` | `7365461.011 ops/s` | `+2.67%` | `neutral` |
