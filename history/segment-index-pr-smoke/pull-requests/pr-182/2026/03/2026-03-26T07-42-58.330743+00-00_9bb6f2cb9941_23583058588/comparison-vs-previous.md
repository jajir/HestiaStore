# Benchmark Comparison

- Profile: `segment-index-pr-smoke`
- Baseline SHA: `e72f4974ea597aa48d2725b131d13e39bcfa6872`
- Candidate SHA: `9bb6f2cb99419786068e4300a8a22e553c77b15d`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `segment-index-get-multisegment-hot:getHitAsyncJoin` | `97.043 ops/s` | `100.741 ops/s` | `+3.81%` | `better` |
| `segment-index-get-multisegment-hot:getHitSync` | `84.004 ops/s` | `82.331 ops/s` | `-1.99%` | `neutral` |
| `segment-index-get-multisegment-hot:getMissAsyncJoin` | `165098.087 ops/s` | `186788.654 ops/s` | `+13.14%` | `better` |
| `segment-index-get-multisegment-hot:getMissSync` | `3826025.739 ops/s` | `3882238.471 ops/s` | `+1.47%` | `neutral` |
| `segment-index-get-overlay:getHitAsyncJoin` | `155664.746 ops/s` | `174268.382 ops/s` | `+11.95%` | `better` |
| `segment-index-get-overlay:getHitSync` | `4213435.612 ops/s` | `4211349.649 ops/s` | `-0.05%` | `neutral` |
| `segment-index-get-overlay:getMissAsyncJoin` | `156455.503 ops/s` | `168848.027 ops/s` | `+7.92%` | `better` |
| `segment-index-get-overlay:getMissSync` | `4124415.193 ops/s` | `3900194.694 ops/s` | `-5.44%` | `warning` |
| `segment-index-get-persisted:getHitAsyncJoin` | `55789.753 ops/s` | `56782.498 ops/s` | `+1.78%` | `neutral` |
| `segment-index-get-persisted:getHitSync` | `104396.145 ops/s` | `107650.031 ops/s` | `+3.12%` | `better` |
| `segment-index-get-persisted:getMissAsyncJoin` | `165994.696 ops/s` | `167447.738 ops/s` | `+0.88%` | `neutral` |
| `segment-index-get-persisted:getMissSync` | `3940414.940 ops/s` | `3635039.536 ops/s` | `-7.75%` | `worse` |
| `segment-index-hot-partition-put:putHotPartition` | `3124721.140 ops/s` | `3044492.040 ops/s` | `-2.57%` | `neutral` |
| `segment-index-hot-partition-put:putThenGetHotPartition` | `1717560.089 ops/s` | `1657588.241 ops/s` | `-3.49%` | `warning` |
| `segment-index-mixed-drain:partitionedIngestMixed` | `455608.507 ops/s` | `448869.580 ops/s` | `-1.48%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed:getWorkload` | `450389.833 ops/s` | `443509.044 ops/s` | `-1.53%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed:putWorkload` | `5218.674 ops/s` | `5360.536 ops/s` | `+2.72%` | `neutral` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed` | `207684.917 ops/s` | `209593.181 ops/s` | `+0.92%` | `neutral` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:getWorkload` | `205044.275 ops/s` | `207332.257 ops/s` | `+1.12%` | `neutral` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:putWorkload` | `2640.642 ops/s` | `2260.924 ops/s` | `-14.38%` | `worse` |
| `segment-index-persisted-mutation:deleteAsyncJoin` | `2160.231 ops/s` | `1742.652 ops/s` | `-19.33%` | `worse` |
| `segment-index-persisted-mutation:deleteAsyncJoin:diag_directoryCountDelta` | `0.000 ops/s` | `-` | `-` | `removed` |
| `segment-index-persisted-mutation:deleteAsyncJoin:diag_fileBytesDelta` | `54491.914 ops/s` | `-` | `-` | `removed` |
| `segment-index-persisted-mutation:deleteAsyncJoin:diag_fileCountDelta` | `3.999 ops/s` | `-` | `-` | `removed` |
| `segment-index-persisted-mutation:deleteSync` | `2479.974 ops/s` | `1939.360 ops/s` | `-21.80%` | `worse` |
| `segment-index-persisted-mutation:deleteSync:diag_directoryCountDelta` | `0.000 ops/s` | `-` | `-` | `removed` |
| `segment-index-persisted-mutation:deleteSync:diag_fileBytesDelta` | `58277.041 ops/s` | `-` | `-` | `removed` |
| `segment-index-persisted-mutation:deleteSync:diag_fileCountDelta` | `3.332 ops/s` | `-` | `-` | `removed` |
| `segment-index-persisted-mutation:putAsyncJoin` | `2215.411 ops/s` | `1842.452 ops/s` | `-16.83%` | `worse` |
| `segment-index-persisted-mutation:putAsyncJoin:diag_directoryCountDelta` | `0.000 ops/s` | `-` | `-` | `removed` |
| `segment-index-persisted-mutation:putAsyncJoin:diag_fileBytesDelta` | `230876.647 ops/s` | `-` | `-` | `removed` |
| `segment-index-persisted-mutation:putAsyncJoin:diag_fileCountDelta` | `3.999 ops/s` | `-` | `-` | `removed` |
| `segment-index-persisted-mutation:putSync` | `2514.868 ops/s` | `2200.018 ops/s` | `-12.52%` | `worse` |
| `segment-index-persisted-mutation:putSync:diag_directoryCountDelta` | `0.000 ops/s` | `-` | `-` | `removed` |
| `segment-index-persisted-mutation:putSync:diag_fileBytesDelta` | `258610.600 ops/s` | `-` | `-` | `removed` |
| `segment-index-persisted-mutation:putSync:diag_fileCountDelta` | `2.982 ops/s` | `-` | `-` | `removed` |
| `single-chunk-entry-write:writeEntrySteadyState` | `7932313.730 ops/s` | `-` | `-` | `removed` |
| `sorted-data-diff-key-read:readNextKey` | `6487103.344 ops/s` | `-` | `-` | `removed` |
