# Benchmark Comparison

- Profile: `segment-index-pr-smoke`
- Baseline SHA: `e72f4974ea597aa48d2725b131d13e39bcfa6872`
- Candidate SHA: `e7bd97726a15b317214d33314fe082cdec5c0f73`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `segment-index-get-multisegment-hot:getHitAsyncJoin` | `97.043 ops/s` | `90.193 ops/s` | `-7.06%` | `worse` |
| `segment-index-get-multisegment-hot:getHitSync` | `84.004 ops/s` | `82.054 ops/s` | `-2.32%` | `neutral` |
| `segment-index-get-multisegment-hot:getMissAsyncJoin` | `165098.087 ops/s` | `234947.834 ops/s` | `+42.31%` | `better` |
| `segment-index-get-multisegment-hot:getMissSync` | `3826025.739 ops/s` | `2342603.622 ops/s` | `-38.77%` | `worse` |
| `segment-index-get-overlay:getHitAsyncJoin` | `155664.746 ops/s` | `227537.498 ops/s` | `+46.17%` | `better` |
| `segment-index-get-overlay:getHitSync` | `4213435.612 ops/s` | `2761147.918 ops/s` | `-34.47%` | `worse` |
| `segment-index-get-overlay:getMissAsyncJoin` | `156455.503 ops/s` | `235510.024 ops/s` | `+50.53%` | `better` |
| `segment-index-get-overlay:getMissSync` | `4124415.193 ops/s` | `2433374.178 ops/s` | `-41.00%` | `worse` |
| `segment-index-get-persisted:getHitAsyncJoin` | `55789.753 ops/s` | `64006.523 ops/s` | `+14.73%` | `better` |
| `segment-index-get-persisted:getHitSync` | `104396.145 ops/s` | `117313.663 ops/s` | `+12.37%` | `better` |
| `segment-index-get-persisted:getMissAsyncJoin` | `165994.696 ops/s` | `219245.536 ops/s` | `+32.08%` | `better` |
| `segment-index-get-persisted:getMissSync` | `3940414.940 ops/s` | `2465712.542 ops/s` | `-37.43%` | `worse` |
| `segment-index-hot-partition-put:putHotPartition` | `3124721.140 ops/s` | `2088175.225 ops/s` | `-33.17%` | `worse` |
| `segment-index-hot-partition-put:putThenGetHotPartition` | `1717560.089 ops/s` | `1142176.615 ops/s` | `-33.50%` | `worse` |
| `segment-index-mixed-drain:partitionedIngestMixed` | `455608.507 ops/s` | `399038.709 ops/s` | `-12.42%` | `worse` |
| `segment-index-mixed-drain:partitionedIngestMixed:getWorkload` | `450389.833 ops/s` | `393847.751 ops/s` | `-12.55%` | `worse` |
| `segment-index-mixed-drain:partitionedIngestMixed:putWorkload` | `5218.674 ops/s` | `5190.958 ops/s` | `-0.53%` | `neutral` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed` | `207684.917 ops/s` | `173711.537 ops/s` | `-16.36%` | `worse` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:getWorkload` | `205044.275 ops/s` | `171217.123 ops/s` | `-16.50%` | `worse` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:putWorkload` | `2640.642 ops/s` | `2494.414 ops/s` | `-5.54%` | `warning` |
| `segment-index-persisted-mutation:deleteAsyncJoin` | `2160.231 ops/s` | `2819.302 ops/s` | `+30.51%` | `better` |
| `segment-index-persisted-mutation:deleteAsyncJoin:diag_directoryCountDelta` | `0.000 ops/s` | `-` | `-` | `removed` |
| `segment-index-persisted-mutation:deleteAsyncJoin:diag_fileBytesDelta` | `54491.914 ops/s` | `-` | `-` | `removed` |
| `segment-index-persisted-mutation:deleteAsyncJoin:diag_fileCountDelta` | `3.999 ops/s` | `-` | `-` | `removed` |
| `segment-index-persisted-mutation:deleteSync` | `2479.974 ops/s` | `3125.684 ops/s` | `+26.04%` | `better` |
| `segment-index-persisted-mutation:deleteSync:diag_directoryCountDelta` | `0.000 ops/s` | `-` | `-` | `removed` |
| `segment-index-persisted-mutation:deleteSync:diag_fileBytesDelta` | `58277.041 ops/s` | `-` | `-` | `removed` |
| `segment-index-persisted-mutation:deleteSync:diag_fileCountDelta` | `3.332 ops/s` | `-` | `-` | `removed` |
| `segment-index-persisted-mutation:putAsyncJoin` | `2215.411 ops/s` | `2695.856 ops/s` | `+21.69%` | `better` |
| `segment-index-persisted-mutation:putAsyncJoin:diag_directoryCountDelta` | `0.000 ops/s` | `-` | `-` | `removed` |
| `segment-index-persisted-mutation:putAsyncJoin:diag_fileBytesDelta` | `230876.647 ops/s` | `-` | `-` | `removed` |
| `segment-index-persisted-mutation:putAsyncJoin:diag_fileCountDelta` | `3.999 ops/s` | `-` | `-` | `removed` |
| `segment-index-persisted-mutation:putSync` | `2514.868 ops/s` | `3012.945 ops/s` | `+19.81%` | `better` |
| `segment-index-persisted-mutation:putSync:diag_directoryCountDelta` | `0.000 ops/s` | `-` | `-` | `removed` |
| `segment-index-persisted-mutation:putSync:diag_fileBytesDelta` | `258610.600 ops/s` | `-` | `-` | `removed` |
| `segment-index-persisted-mutation:putSync:diag_fileCountDelta` | `2.982 ops/s` | `-` | `-` | `removed` |
| `single-chunk-entry-write:writeEntrySteadyState` | `7932313.730 ops/s` | `-` | `-` | `removed` |
| `sorted-data-diff-key-read:readNextKey` | `6487103.344 ops/s` | `-` | `-` | `removed` |
