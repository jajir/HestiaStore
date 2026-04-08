# Benchmark Comparison

- Profile: `segment-index-pr-smoke`
- Baseline SHA: `e72f4974ea597aa48d2725b131d13e39bcfa6872`
- Candidate SHA: `e285ee0658e4ba27ca839f89225b8d123ef8f891`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `segment-index-get-multisegment-hot:getHitAsyncJoin` | `97.043 ops/s` | `96.050 ops/s` | `-1.02%` | `neutral` |
| `segment-index-get-multisegment-hot:getHitSync` | `84.004 ops/s` | `83.167 ops/s` | `-1.00%` | `neutral` |
| `segment-index-get-multisegment-hot:getMissAsyncJoin` | `165098.087 ops/s` | `184676.867 ops/s` | `+11.86%` | `better` |
| `segment-index-get-multisegment-hot:getMissSync` | `3826025.739 ops/s` | `3716949.093 ops/s` | `-2.85%` | `neutral` |
| `segment-index-get-overlay:getHitAsyncJoin` | `155664.746 ops/s` | `180480.340 ops/s` | `+15.94%` | `better` |
| `segment-index-get-overlay:getHitSync` | `4213435.612 ops/s` | `4099984.699 ops/s` | `-2.69%` | `neutral` |
| `segment-index-get-overlay:getMissAsyncJoin` | `156455.503 ops/s` | `182196.782 ops/s` | `+16.45%` | `better` |
| `segment-index-get-overlay:getMissSync` | `4124415.193 ops/s` | `3761884.750 ops/s` | `-8.79%` | `worse` |
| `segment-index-get-persisted:getHitAsyncJoin` | `55789.753 ops/s` | `63466.090 ops/s` | `+13.76%` | `better` |
| `segment-index-get-persisted:getHitSync` | `104396.145 ops/s` | `122170.647 ops/s` | `+17.03%` | `better` |
| `segment-index-get-persisted:getMissAsyncJoin` | `165994.696 ops/s` | `186175.687 ops/s` | `+12.16%` | `better` |
| `segment-index-get-persisted:getMissSync` | `3940414.940 ops/s` | `3716374.194 ops/s` | `-5.69%` | `warning` |
| `segment-index-hot-partition-put:putHotPartition` | `3124721.140 ops/s` | `2862751.015 ops/s` | `-8.38%` | `worse` |
| `segment-index-hot-partition-put:putThenGetHotPartition` | `1717560.089 ops/s` | `1540143.326 ops/s` | `-10.33%` | `worse` |
| `segment-index-mixed-drain:partitionedIngestMixed` | `455608.507 ops/s` | `389694.431 ops/s` | `-14.47%` | `worse` |
| `segment-index-mixed-drain:partitionedIngestMixed:getWorkload` | `450389.833 ops/s` | `384599.808 ops/s` | `-14.61%` | `worse` |
| `segment-index-mixed-drain:partitionedIngestMixed:putWorkload` | `5218.674 ops/s` | `5094.623 ops/s` | `-2.38%` | `neutral` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed` | `207684.917 ops/s` | `214141.034 ops/s` | `+3.11%` | `better` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:getWorkload` | `205044.275 ops/s` | `211757.092 ops/s` | `+3.27%` | `better` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:putWorkload` | `2640.642 ops/s` | `2383.942 ops/s` | `-9.72%` | `worse` |
| `segment-index-persisted-mutation:deleteAsyncJoin` | `2160.231 ops/s` | `2765.961 ops/s` | `+28.04%` | `better` |
| `segment-index-persisted-mutation:deleteAsyncJoin:diag_directoryCountDelta` | `0.000 ops/s` | `-` | `-` | `removed` |
| `segment-index-persisted-mutation:deleteAsyncJoin:diag_fileBytesDelta` | `54491.914 ops/s` | `-` | `-` | `removed` |
| `segment-index-persisted-mutation:deleteAsyncJoin:diag_fileCountDelta` | `3.999 ops/s` | `-` | `-` | `removed` |
| `segment-index-persisted-mutation:deleteSync` | `2479.974 ops/s` | `3064.007 ops/s` | `+23.55%` | `better` |
| `segment-index-persisted-mutation:deleteSync:diag_directoryCountDelta` | `0.000 ops/s` | `-` | `-` | `removed` |
| `segment-index-persisted-mutation:deleteSync:diag_fileBytesDelta` | `58277.041 ops/s` | `-` | `-` | `removed` |
| `segment-index-persisted-mutation:deleteSync:diag_fileCountDelta` | `3.332 ops/s` | `-` | `-` | `removed` |
| `segment-index-persisted-mutation:putAsyncJoin` | `2215.411 ops/s` | `2712.314 ops/s` | `+22.43%` | `better` |
| `segment-index-persisted-mutation:putAsyncJoin:diag_directoryCountDelta` | `0.000 ops/s` | `-` | `-` | `removed` |
| `segment-index-persisted-mutation:putAsyncJoin:diag_fileBytesDelta` | `230876.647 ops/s` | `-` | `-` | `removed` |
| `segment-index-persisted-mutation:putAsyncJoin:diag_fileCountDelta` | `3.999 ops/s` | `-` | `-` | `removed` |
| `segment-index-persisted-mutation:putSync` | `2514.868 ops/s` | `2992.878 ops/s` | `+19.01%` | `better` |
| `segment-index-persisted-mutation:putSync:diag_directoryCountDelta` | `0.000 ops/s` | `-` | `-` | `removed` |
| `segment-index-persisted-mutation:putSync:diag_fileBytesDelta` | `258610.600 ops/s` | `-` | `-` | `removed` |
| `segment-index-persisted-mutation:putSync:diag_fileCountDelta` | `2.982 ops/s` | `-` | `-` | `removed` |
| `single-chunk-entry-write:writeEntrySteadyState` | `7932313.730 ops/s` | `-` | `-` | `removed` |
| `sorted-data-diff-key-read:readNextKey` | `6487103.344 ops/s` | `-` | `-` | `removed` |
