# Benchmark Comparison

- Candidate labels rerun 3x total and median-merged because the initial regression exceeded threshold with high relative score error: `segment-index-get-multisegment-cold`


- Profile: `segment-index-nightly`
- Baseline SHA: `e72f4974ea597aa48d2725b131d13e39bcfa6872`
- Candidate SHA: `e72f4974ea597aa48d2725b131d13e39bcfa6872`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `segment-index-get-multisegment-cold:getHitAsyncJoin` | `49.210 ops/s` | `42.426 ops/s` | `-13.79%` | `worse` |
| `segment-index-get-multisegment-cold:getHitSync` | `53.451 ops/s` | `48.495 ops/s` | `-9.27%` | `worse` |
| `segment-index-get-multisegment-cold:getMissAsyncJoin` | `189652.203 ops/s` | `180390.293 ops/s` | `-4.88%` | `warning` |
| `segment-index-get-multisegment-cold:getMissSync` | `3383853.921 ops/s` | `3662235.407 ops/s` | `+8.23%` | `better` |
| `segment-index-get-multisegment-hot:getHitAsyncJoin` | `102.706 ops/s` | `102.066 ops/s` | `-0.62%` | `neutral` |
| `segment-index-get-multisegment-hot:getHitSync` | `93.622 ops/s` | `98.613 ops/s` | `+5.33%` | `better` |
| `segment-index-get-multisegment-hot:getMissAsyncJoin` | `187115.271 ops/s` | `180900.758 ops/s` | `-3.32%` | `warning` |
| `segment-index-get-multisegment-hot:getMissSync` | `3784684.033 ops/s` | `3972218.136 ops/s` | `+4.96%` | `better` |
| `segment-index-get-overlay:getHitAsyncJoin` | `173290.313 ops/s` | `168911.345 ops/s` | `-2.53%` | `neutral` |
| `segment-index-get-overlay:getHitSync` | `4056861.740 ops/s` | `3879716.137 ops/s` | `-4.37%` | `warning` |
| `segment-index-get-overlay:getMissAsyncJoin` | `171412.321 ops/s` | `167957.216 ops/s` | `-2.02%` | `neutral` |
| `segment-index-get-overlay:getMissSync` | `3627570.883 ops/s` | `3678663.268 ops/s` | `+1.41%` | `neutral` |
| `segment-index-get-persisted:getHitAsyncJoin` | `65163.374 ops/s` | `65153.269 ops/s` | `-0.02%` | `neutral` |
| `segment-index-get-persisted:getHitSync` | `117874.457 ops/s` | `118082.223 ops/s` | `+0.18%` | `neutral` |
| `segment-index-get-persisted:getMissAsyncJoin` | `170421.306 ops/s` | `164585.026 ops/s` | `-3.42%` | `warning` |
| `segment-index-get-persisted:getMissSync` | `3849279.951 ops/s` | `3655872.252 ops/s` | `-5.02%` | `warning` |
| `segment-index-hot-partition-put:putHotPartition` | `3105910.846 ops/s` | `2875224.845 ops/s` | `-7.43%` | `worse` |
| `segment-index-hot-partition-put:putThenGetHotPartition` | `1615766.699 ops/s` | `1652642.212 ops/s` | `+2.28%` | `neutral` |
| `segment-index-lifecycle:openAndCheckAndRepairConsistency` | `244.838 ms/op` | `244.789 ms/op` | `-0.02%` | `neutral` |
| `segment-index-lifecycle:openAndCompact` | `266.096 ms/op` | `268.778 ms/op` | `+1.01%` | `neutral` |
| `segment-index-lifecycle:openExisting` | `244.746 ms/op` | `241.139 ms/op` | `-1.47%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed` | `569980.643 ops/s` | `504631.958 ops/s` | `-11.47%` | `worse` |
| `segment-index-mixed-drain:partitionedIngestMixed:getWorkload` | `564631.826 ops/s` | `499348.273 ops/s` | `-11.56%` | `worse` |
| `segment-index-mixed-drain:partitionedIngestMixed:putWorkload` | `5348.817 ops/s` | `5283.685 ops/s` | `-1.22%` | `neutral` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed` | `275434.627 ops/s` | `269459.359 ops/s` | `-2.17%` | `neutral` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:getWorkload` | `274012.423 ops/s` | `268022.847 ops/s` | `-2.19%` | `neutral` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:putWorkload` | `1422.204 ops/s` | `1436.512 ops/s` | `+1.01%` | `neutral` |
| `segment-index-persisted-mutation:deleteAsyncJoin` | `1611.435 ops/s` | `2069.090 ops/s` | `+28.40%` | `better` |
| `segment-index-persisted-mutation:deleteSync` | `1641.004 ops/s` | `2297.408 ops/s` | `+40.00%` | `better` |
| `segment-index-persisted-mutation:putAsyncJoin` | `1531.500 ops/s` | `2029.158 ops/s` | `+32.49%` | `better` |
| `segment-index-persisted-mutation:putSync` | `1600.170 ops/s` | `2250.612 ops/s` | `+40.65%` | `better` |
| `single-chunk-entry-write-compact:writeEntrySteadyState` | `8536502.081 ops/s` | `8517563.106 ops/s` | `-0.22%` | `neutral` |
| `single-chunk-entry-write-large:writeEntrySteadyState` | `7931647.282 ops/s` | `7933211.638 ops/s` | `+0.02%` | `neutral` |
| `sorted-data-diff-key-read-compact:readNextKey` | `9481033.142 ops/s` | `9400514.844 ops/s` | `-0.85%` | `neutral` |
| `sorted-data-diff-key-read-large:readNextKey` | `7504162.838 ops/s` | `7588164.758 ops/s` | `+1.12%` | `neutral` |
