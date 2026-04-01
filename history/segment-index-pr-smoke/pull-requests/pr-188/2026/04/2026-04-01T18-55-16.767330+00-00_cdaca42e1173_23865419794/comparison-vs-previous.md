# Benchmark Comparison

- Profile: `segment-index-pr-smoke`
- Baseline SHA: `e72f4974ea597aa48d2725b131d13e39bcfa6872`
- Candidate SHA: `cdaca42e1173dfdfc42d362dabb748b042f928dc`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `segment-index-get-multisegment-hot:getHitAsyncJoin` | `97.043 ops/s` | `99.439 ops/s` | `+2.47%` | `neutral` |
| `segment-index-get-multisegment-hot:getHitSync` | `84.004 ops/s` | `88.676 ops/s` | `+5.56%` | `better` |
| `segment-index-get-multisegment-hot:getMissAsyncJoin` | `165098.087 ops/s` | `169647.541 ops/s` | `+2.76%` | `neutral` |
| `segment-index-get-multisegment-hot:getMissSync` | `3826025.739 ops/s` | `3925779.645 ops/s` | `+2.61%` | `neutral` |
| `segment-index-get-overlay:getHitAsyncJoin` | `155664.746 ops/s` | `154985.718 ops/s` | `-0.44%` | `neutral` |
| `segment-index-get-overlay:getHitSync` | `4213435.612 ops/s` | `4147800.107 ops/s` | `-1.56%` | `neutral` |
| `segment-index-get-overlay:getMissAsyncJoin` | `156455.503 ops/s` | `154827.981 ops/s` | `-1.04%` | `neutral` |
| `segment-index-get-overlay:getMissSync` | `4124415.193 ops/s` | `3948384.385 ops/s` | `-4.27%` | `warning` |
| `segment-index-get-persisted:getHitAsyncJoin` | `55789.753 ops/s` | `52840.677 ops/s` | `-5.29%` | `warning` |
| `segment-index-get-persisted:getHitSync` | `104396.145 ops/s` | `118573.322 ops/s` | `+13.58%` | `better` |
| `segment-index-get-persisted:getMissAsyncJoin` | `165994.696 ops/s` | `164466.754 ops/s` | `-0.92%` | `neutral` |
| `segment-index-get-persisted:getMissSync` | `3940414.940 ops/s` | `4012347.007 ops/s` | `+1.83%` | `neutral` |
| `segment-index-hot-partition-put:putHotPartition` | `3124721.140 ops/s` | `2869807.515 ops/s` | `-8.16%` | `worse` |
| `segment-index-hot-partition-put:putThenGetHotPartition` | `1717560.089 ops/s` | `1647185.793 ops/s` | `-4.10%` | `warning` |
| `segment-index-mixed-drain:partitionedIngestMixed` | `455608.507 ops/s` | `391577.024 ops/s` | `-14.05%` | `worse` |
| `segment-index-mixed-drain:partitionedIngestMixed:getWorkload` | `450389.833 ops/s` | `386483.772 ops/s` | `-14.19%` | `worse` |
| `segment-index-mixed-drain:partitionedIngestMixed:putWorkload` | `5218.674 ops/s` | `5093.252 ops/s` | `-2.40%` | `neutral` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed` | `207684.917 ops/s` | `191488.920 ops/s` | `-7.80%` | `worse` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:getWorkload` | `205044.275 ops/s` | `189057.865 ops/s` | `-7.80%` | `worse` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:putWorkload` | `2640.642 ops/s` | `2431.054 ops/s` | `-7.94%` | `worse` |
| `segment-index-persisted-mutation:deleteAsyncJoin` | `2160.231 ops/s` | `2089.064 ops/s` | `-3.29%` | `warning` |
| `segment-index-persisted-mutation:deleteAsyncJoin:diag_directoryCountDelta` | `0.000 ops/s` | `-` | `-` | `removed` |
| `segment-index-persisted-mutation:deleteAsyncJoin:diag_fileBytesDelta` | `54491.914 ops/s` | `-` | `-` | `removed` |
| `segment-index-persisted-mutation:deleteAsyncJoin:diag_fileCountDelta` | `3.999 ops/s` | `-` | `-` | `removed` |
| `segment-index-persisted-mutation:deleteSync` | `2479.974 ops/s` | `2372.370 ops/s` | `-4.34%` | `warning` |
| `segment-index-persisted-mutation:deleteSync:diag_directoryCountDelta` | `0.000 ops/s` | `-` | `-` | `removed` |
| `segment-index-persisted-mutation:deleteSync:diag_fileBytesDelta` | `58277.041 ops/s` | `-` | `-` | `removed` |
| `segment-index-persisted-mutation:deleteSync:diag_fileCountDelta` | `3.332 ops/s` | `-` | `-` | `removed` |
| `segment-index-persisted-mutation:putAsyncJoin` | `2215.411 ops/s` | `1948.324 ops/s` | `-12.06%` | `worse` |
| `segment-index-persisted-mutation:putAsyncJoin:diag_directoryCountDelta` | `0.000 ops/s` | `-` | `-` | `removed` |
| `segment-index-persisted-mutation:putAsyncJoin:diag_fileBytesDelta` | `230876.647 ops/s` | `-` | `-` | `removed` |
| `segment-index-persisted-mutation:putAsyncJoin:diag_fileCountDelta` | `3.999 ops/s` | `-` | `-` | `removed` |
| `segment-index-persisted-mutation:putSync` | `2514.868 ops/s` | `2157.518 ops/s` | `-14.21%` | `worse` |
| `segment-index-persisted-mutation:putSync:diag_directoryCountDelta` | `0.000 ops/s` | `-` | `-` | `removed` |
| `segment-index-persisted-mutation:putSync:diag_fileBytesDelta` | `258610.600 ops/s` | `-` | `-` | `removed` |
| `segment-index-persisted-mutation:putSync:diag_fileCountDelta` | `2.982 ops/s` | `-` | `-` | `removed` |
| `single-chunk-entry-write:writeEntrySteadyState` | `7932313.730 ops/s` | `-` | `-` | `removed` |
| `sorted-data-diff-key-read:readNextKey` | `6487103.344 ops/s` | `-` | `-` | `removed` |
