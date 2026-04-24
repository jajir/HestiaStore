# Benchmark Comparison

- Candidate labels rerun 3x total and median-merged because the initial regression exceeded threshold with high relative score error: `segment-index-get-multisegment-hot,segment-index-persisted-mutation`


- Profile: `segment-index-nightly`
- Baseline SHA: `a87e6e844c0f83bfc93f8ff3748153d6ffec3dbb`
- Candidate SHA: `e72f4974ea597aa48d2725b131d13e39bcfa6872`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `segment-index-get-multisegment-cold:getHitAsyncJoin` | `36.538 ops/s` | `40.628 ops/s` | `+11.19%` | `better` |
| `segment-index-get-multisegment-cold:getHitSync` | `37.131 ops/s` | `41.967 ops/s` | `+13.02%` | `better` |
| `segment-index-get-multisegment-cold:getMissAsyncJoin` | `240210.689 ops/s` | `237396.332 ops/s` | `-1.17%` | `neutral` |
| `segment-index-get-multisegment-cold:getMissSync` | `4593155.143 ops/s` | `5059055.894 ops/s` | `+10.14%` | `better` |
| `segment-index-get-multisegment-hot:getHitAsyncJoin` | `89.922 ops/s` | `90.098 ops/s` | `+0.20%` | `neutral` |
| `segment-index-get-multisegment-hot:getHitSync` | `86.169 ops/s` | `79.926 ops/s` | `-7.24%` | `worse` |
| `segment-index-get-multisegment-hot:getMissAsyncJoin` | `242092.204 ops/s` | `239645.279 ops/s` | `-1.01%` | `neutral` |
| `segment-index-get-multisegment-hot:getMissSync` | `4942628.158 ops/s` | `5016677.151 ops/s` | `+1.50%` | `neutral` |
| `segment-index-get-overlay:getHitAsyncJoin` | `238846.084 ops/s` | `232647.898 ops/s` | `-2.60%` | `neutral` |
| `segment-index-get-overlay:getHitSync` | `5327220.907 ops/s` | `4581589.548 ops/s` | `-14.00%` | `worse` |
| `segment-index-get-overlay:getMissAsyncJoin` | `239884.739 ops/s` | `240187.888 ops/s` | `+0.13%` | `neutral` |
| `segment-index-get-overlay:getMissSync` | `4326783.063 ops/s` | `4695337.807 ops/s` | `+8.52%` | `better` |
| `segment-index-get-persisted:getHitAsyncJoin` | `73013.275 ops/s` | `73880.716 ops/s` | `+1.19%` | `neutral` |
| `segment-index-get-persisted:getHitSync` | `157747.542 ops/s` | `159055.686 ops/s` | `+0.83%` | `neutral` |
| `segment-index-get-persisted:getMissAsyncJoin` | `233067.822 ops/s` | `240957.906 ops/s` | `+3.39%` | `better` |
| `segment-index-get-persisted:getMissSync` | `4594301.621 ops/s` | `4578793.502 ops/s` | `-0.34%` | `neutral` |
| `segment-index-hot-partition-put:putHotPartition` | `3635668.623 ops/s` | `3955162.399 ops/s` | `+8.79%` | `better` |
| `segment-index-hot-partition-put:putThenGetHotPartition` | `2054738.517 ops/s` | `2144556.632 ops/s` | `+4.37%` | `better` |
| `segment-index-lifecycle:openAndCheckAndRepairConsistency` | `216.575 ms/op` | `216.577 ms/op` | `+0.00%` | `neutral` |
| `segment-index-lifecycle:openAndCompact` | `233.299 ms/op` | `232.944 ms/op` | `-0.15%` | `neutral` |
| `segment-index-lifecycle:openExisting` | `212.314 ms/op` | `215.473 ms/op` | `+1.49%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed` | `766442.827 ops/s` | `779724.495 ops/s` | `+1.73%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed:getWorkload` | `761053.008 ops/s` | `774378.562 ops/s` | `+1.75%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed:putWorkload` | `5389.818 ops/s` | `5345.934 ops/s` | `-0.81%` | `neutral` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed` | `385976.676 ops/s` | `415812.259 ops/s` | `+7.73%` | `better` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:getWorkload` | `384327.307 ops/s` | `362211.585 ops/s` | `-5.75%` | `warning` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:putWorkload` | `1649.370 ops/s` | `53600.675 ops/s` | `+3149.77%` | `better` |
| `segment-index-persisted-mutation:deleteAsyncJoin` | `1411.406 ops/s` | `747.610 ops/s` | `-47.03%` | `worse` |
| `segment-index-persisted-mutation:deleteSync` | `1295.204 ops/s` | `511.916 ops/s` | `-60.48%` | `worse` |
| `segment-index-persisted-mutation:putAsyncJoin` | `1823.997 ops/s` | `970.985 ops/s` | `-46.77%` | `worse` |
| `segment-index-persisted-mutation:putSync` | `2237.667 ops/s` | `790.179 ops/s` | `-64.69%` | `worse` |
| `single-chunk-entry-write-compact:writeEntrySteadyState` | `11021345.439 ops/s` | `11028472.941 ops/s` | `+0.06%` | `neutral` |
| `single-chunk-entry-write-large:writeEntrySteadyState` | `10243433.164 ops/s` | `10196209.302 ops/s` | `-0.46%` | `neutral` |
| `sorted-data-diff-key-read-compact:readNextKey` | `10972799.459 ops/s` | `10957876.558 ops/s` | `-0.14%` | `neutral` |
| `sorted-data-diff-key-read-large:readNextKey` | `8572729.901 ops/s` | `8580481.806 ops/s` | `+0.09%` | `neutral` |
