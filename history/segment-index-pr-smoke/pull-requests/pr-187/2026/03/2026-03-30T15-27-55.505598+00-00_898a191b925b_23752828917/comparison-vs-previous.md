# Benchmark Comparison

- Profile: `segment-index-pr-smoke`
- Baseline SHA: `e72f4974ea597aa48d2725b131d13e39bcfa6872`
- Candidate SHA: `898a191b925bce743b80fb0913b1329202b5001a`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `segment-index-get-multisegment-hot:getHitAsyncJoin` | `97.043 ops/s` | `91.362 ops/s` | `-5.85%` | `warning` |
| `segment-index-get-multisegment-hot:getHitSync` | `84.004 ops/s` | `91.012 ops/s` | `+8.34%` | `better` |
| `segment-index-get-multisegment-hot:getMissAsyncJoin` | `165098.087 ops/s` | `240752.942 ops/s` | `+45.82%` | `better` |
| `segment-index-get-multisegment-hot:getMissSync` | `3826025.739 ops/s` | `2602782.263 ops/s` | `-31.97%` | `worse` |
| `segment-index-get-overlay:getHitAsyncJoin` | `155664.746 ops/s` | `228949.450 ops/s` | `+47.08%` | `better` |
| `segment-index-get-overlay:getHitSync` | `4213435.612 ops/s` | `2820356.388 ops/s` | `-33.06%` | `worse` |
| `segment-index-get-overlay:getMissAsyncJoin` | `156455.503 ops/s` | `240419.690 ops/s` | `+53.67%` | `better` |
| `segment-index-get-overlay:getMissSync` | `4124415.193 ops/s` | `2575400.329 ops/s` | `-37.56%` | `worse` |
| `segment-index-get-persisted:getHitAsyncJoin` | `55789.753 ops/s` | `56621.064 ops/s` | `+1.49%` | `neutral` |
| `segment-index-get-persisted:getHitSync` | `104396.145 ops/s` | `109794.027 ops/s` | `+5.17%` | `better` |
| `segment-index-get-persisted:getMissAsyncJoin` | `165994.696 ops/s` | `224920.916 ops/s` | `+35.50%` | `better` |
| `segment-index-get-persisted:getMissSync` | `3940414.940 ops/s` | `2496954.354 ops/s` | `-36.63%` | `worse` |
| `segment-index-hot-partition-put:putHotPartition` | `3124721.140 ops/s` | `1944859.108 ops/s` | `-37.76%` | `worse` |
| `segment-index-hot-partition-put:putThenGetHotPartition` | `1717560.089 ops/s` | `1135668.036 ops/s` | `-33.88%` | `worse` |
| `segment-index-mixed-drain:partitionedIngestMixed` | `455608.507 ops/s` | `402806.757 ops/s` | `-11.59%` | `worse` |
| `segment-index-mixed-drain:partitionedIngestMixed:getWorkload` | `450389.833 ops/s` | `397435.608 ops/s` | `-11.76%` | `worse` |
| `segment-index-mixed-drain:partitionedIngestMixed:putWorkload` | `5218.674 ops/s` | `5371.149 ops/s` | `+2.92%` | `neutral` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed` | `207684.917 ops/s` | `173638.214 ops/s` | `-16.39%` | `worse` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:getWorkload` | `205044.275 ops/s` | `171042.474 ops/s` | `-16.58%` | `worse` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:putWorkload` | `2640.642 ops/s` | `2595.740 ops/s` | `-1.70%` | `neutral` |
| `segment-index-persisted-mutation:deleteAsyncJoin` | `2160.231 ops/s` | `2758.180 ops/s` | `+27.68%` | `better` |
| `segment-index-persisted-mutation:deleteAsyncJoin:diag_directoryCountDelta` | `0.000 ops/s` | `-` | `-` | `removed` |
| `segment-index-persisted-mutation:deleteAsyncJoin:diag_fileBytesDelta` | `54491.914 ops/s` | `-` | `-` | `removed` |
| `segment-index-persisted-mutation:deleteAsyncJoin:diag_fileCountDelta` | `3.999 ops/s` | `-` | `-` | `removed` |
| `segment-index-persisted-mutation:deleteSync` | `2479.974 ops/s` | `3228.231 ops/s` | `+30.17%` | `better` |
| `segment-index-persisted-mutation:deleteSync:diag_directoryCountDelta` | `0.000 ops/s` | `-` | `-` | `removed` |
| `segment-index-persisted-mutation:deleteSync:diag_fileBytesDelta` | `58277.041 ops/s` | `-` | `-` | `removed` |
| `segment-index-persisted-mutation:deleteSync:diag_fileCountDelta` | `3.332 ops/s` | `-` | `-` | `removed` |
| `segment-index-persisted-mutation:putAsyncJoin` | `2215.411 ops/s` | `2736.776 ops/s` | `+23.53%` | `better` |
| `segment-index-persisted-mutation:putAsyncJoin:diag_directoryCountDelta` | `0.000 ops/s` | `-` | `-` | `removed` |
| `segment-index-persisted-mutation:putAsyncJoin:diag_fileBytesDelta` | `230876.647 ops/s` | `-` | `-` | `removed` |
| `segment-index-persisted-mutation:putAsyncJoin:diag_fileCountDelta` | `3.999 ops/s` | `-` | `-` | `removed` |
| `segment-index-persisted-mutation:putSync` | `2514.868 ops/s` | `3112.089 ops/s` | `+23.75%` | `better` |
| `segment-index-persisted-mutation:putSync:diag_directoryCountDelta` | `0.000 ops/s` | `-` | `-` | `removed` |
| `segment-index-persisted-mutation:putSync:diag_fileBytesDelta` | `258610.600 ops/s` | `-` | `-` | `removed` |
| `segment-index-persisted-mutation:putSync:diag_fileCountDelta` | `2.982 ops/s` | `-` | `-` | `removed` |
| `single-chunk-entry-write:writeEntrySteadyState` | `7932313.730 ops/s` | `-` | `-` | `removed` |
| `sorted-data-diff-key-read:readNextKey` | `6487103.344 ops/s` | `-` | `-` | `removed` |
