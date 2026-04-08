# Benchmark Comparison

- Profile: `segment-index-pr-smoke`
- Baseline SHA: `e72f4974ea597aa48d2725b131d13e39bcfa6872`
- Candidate SHA: `e4c4a66109d1467c9f7f82ccc9cb5eaaff3b6e36`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `segment-index-get-multisegment-hot:getHitAsyncJoin` | `97.043 ops/s` | `88.475 ops/s` | `-8.83%` | `worse` |
| `segment-index-get-multisegment-hot:getHitSync` | `84.004 ops/s` | `81.663 ops/s` | `-2.79%` | `neutral` |
| `segment-index-get-multisegment-hot:getMissAsyncJoin` | `165098.087 ops/s` | `170362.969 ops/s` | `+3.19%` | `better` |
| `segment-index-get-multisegment-hot:getMissSync` | `3826025.739 ops/s` | `3770678.989 ops/s` | `-1.45%` | `neutral` |
| `segment-index-get-overlay:getHitAsyncJoin` | `155664.746 ops/s` | `153769.951 ops/s` | `-1.22%` | `neutral` |
| `segment-index-get-overlay:getHitSync` | `4213435.612 ops/s` | `4130732.412 ops/s` | `-1.96%` | `neutral` |
| `segment-index-get-overlay:getMissAsyncJoin` | `156455.503 ops/s` | `162171.738 ops/s` | `+3.65%` | `better` |
| `segment-index-get-overlay:getMissSync` | `4124415.193 ops/s` | `4005316.007 ops/s` | `-2.89%` | `neutral` |
| `segment-index-get-persisted:getHitAsyncJoin` | `55789.753 ops/s` | `55080.111 ops/s` | `-1.27%` | `neutral` |
| `segment-index-get-persisted:getHitSync` | `104396.145 ops/s` | `122613.449 ops/s` | `+17.45%` | `better` |
| `segment-index-get-persisted:getMissAsyncJoin` | `165994.696 ops/s` | `157282.389 ops/s` | `-5.25%` | `warning` |
| `segment-index-get-persisted:getMissSync` | `3940414.940 ops/s` | `3930823.677 ops/s` | `-0.24%` | `neutral` |
| `segment-index-hot-partition-put:putHotPartition` | `3124721.140 ops/s` | `3015849.400 ops/s` | `-3.48%` | `warning` |
| `segment-index-hot-partition-put:putThenGetHotPartition` | `1717560.089 ops/s` | `1603739.385 ops/s` | `-6.63%` | `warning` |
| `segment-index-mixed-drain:partitionedIngestMixed` | `455608.507 ops/s` | `405629.771 ops/s` | `-10.97%` | `worse` |
| `segment-index-mixed-drain:partitionedIngestMixed:getWorkload` | `450389.833 ops/s` | `400585.089 ops/s` | `-11.06%` | `worse` |
| `segment-index-mixed-drain:partitionedIngestMixed:putWorkload` | `5218.674 ops/s` | `5044.682 ops/s` | `-3.33%` | `warning` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed` | `207684.917 ops/s` | `173871.980 ops/s` | `-16.28%` | `worse` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:getWorkload` | `205044.275 ops/s` | `171299.544 ops/s` | `-16.46%` | `worse` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:putWorkload` | `2640.642 ops/s` | `2572.436 ops/s` | `-2.58%` | `neutral` |
| `segment-index-persisted-mutation:deleteAsyncJoin` | `2160.231 ops/s` | `2209.039 ops/s` | `+2.26%` | `neutral` |
| `segment-index-persisted-mutation:deleteAsyncJoin:diag_directoryCountDelta` | `0.000 ops/s` | `-` | `-` | `removed` |
| `segment-index-persisted-mutation:deleteAsyncJoin:diag_fileBytesDelta` | `54491.914 ops/s` | `-` | `-` | `removed` |
| `segment-index-persisted-mutation:deleteAsyncJoin:diag_fileCountDelta` | `3.999 ops/s` | `-` | `-` | `removed` |
| `segment-index-persisted-mutation:deleteSync` | `2479.974 ops/s` | `2614.905 ops/s` | `+5.44%` | `better` |
| `segment-index-persisted-mutation:deleteSync:diag_directoryCountDelta` | `0.000 ops/s` | `-` | `-` | `removed` |
| `segment-index-persisted-mutation:deleteSync:diag_fileBytesDelta` | `58277.041 ops/s` | `-` | `-` | `removed` |
| `segment-index-persisted-mutation:deleteSync:diag_fileCountDelta` | `3.332 ops/s` | `-` | `-` | `removed` |
| `segment-index-persisted-mutation:putAsyncJoin` | `2215.411 ops/s` | `2268.068 ops/s` | `+2.38%` | `neutral` |
| `segment-index-persisted-mutation:putAsyncJoin:diag_directoryCountDelta` | `0.000 ops/s` | `-` | `-` | `removed` |
| `segment-index-persisted-mutation:putAsyncJoin:diag_fileBytesDelta` | `230876.647 ops/s` | `-` | `-` | `removed` |
| `segment-index-persisted-mutation:putAsyncJoin:diag_fileCountDelta` | `3.999 ops/s` | `-` | `-` | `removed` |
| `segment-index-persisted-mutation:putSync` | `2514.868 ops/s` | `2434.805 ops/s` | `-3.18%` | `warning` |
| `segment-index-persisted-mutation:putSync:diag_directoryCountDelta` | `0.000 ops/s` | `-` | `-` | `removed` |
| `segment-index-persisted-mutation:putSync:diag_fileBytesDelta` | `258610.600 ops/s` | `-` | `-` | `removed` |
| `segment-index-persisted-mutation:putSync:diag_fileCountDelta` | `2.982 ops/s` | `-` | `-` | `removed` |
| `single-chunk-entry-write:writeEntrySteadyState` | `7932313.730 ops/s` | `-` | `-` | `removed` |
| `sorted-data-diff-key-read:readNextKey` | `6487103.344 ops/s` | `-` | `-` | `removed` |
