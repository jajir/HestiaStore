# Benchmark Comparison

- Profile: `segment-index-pr-smoke`
- Baseline SHA: `e72f4974ea597aa48d2725b131d13e39bcfa6872`
- Candidate SHA: `fe1092100a3042f8f623360f76df6d9e21a163fa`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `segment-index-get-multisegment-hot:getHitAsyncJoin` | `97.043 ops/s` | `87.074 ops/s` | `-10.27%` | `worse` |
| `segment-index-get-multisegment-hot:getHitSync` | `84.004 ops/s` | `93.679 ops/s` | `+11.52%` | `better` |
| `segment-index-get-multisegment-hot:getMissAsyncJoin` | `165098.087 ops/s` | `168148.321 ops/s` | `+1.85%` | `neutral` |
| `segment-index-get-multisegment-hot:getMissSync` | `3826025.739 ops/s` | `3871752.477 ops/s` | `+1.20%` | `neutral` |
| `segment-index-get-overlay:getHitAsyncJoin` | `155664.746 ops/s` | `159593.568 ops/s` | `+2.52%` | `neutral` |
| `segment-index-get-overlay:getHitSync` | `4213435.612 ops/s` | `4207938.551 ops/s` | `-0.13%` | `neutral` |
| `segment-index-get-overlay:getMissAsyncJoin` | `156455.503 ops/s` | `157441.798 ops/s` | `+0.63%` | `neutral` |
| `segment-index-get-overlay:getMissSync` | `4124415.193 ops/s` | `3933255.092 ops/s` | `-4.63%` | `warning` |
| `segment-index-get-persisted:getHitAsyncJoin` | `55789.753 ops/s` | `59572.172 ops/s` | `+6.78%` | `better` |
| `segment-index-get-persisted:getHitSync` | `104396.145 ops/s` | `115112.633 ops/s` | `+10.27%` | `better` |
| `segment-index-get-persisted:getMissAsyncJoin` | `165994.696 ops/s` | `161080.353 ops/s` | `-2.96%` | `neutral` |
| `segment-index-get-persisted:getMissSync` | `3940414.940 ops/s` | `3970756.521 ops/s` | `+0.77%` | `neutral` |
| `segment-index-hot-partition-put:putHotPartition` | `3124721.140 ops/s` | `2892545.953 ops/s` | `-7.43%` | `worse` |
| `segment-index-hot-partition-put:putThenGetHotPartition` | `1717560.089 ops/s` | `1562172.393 ops/s` | `-9.05%` | `worse` |
| `segment-index-mixed-drain:partitionedIngestMixed` | `455608.507 ops/s` | `412656.443 ops/s` | `-9.43%` | `worse` |
| `segment-index-mixed-drain:partitionedIngestMixed:getWorkload` | `450389.833 ops/s` | `407693.822 ops/s` | `-9.48%` | `worse` |
| `segment-index-mixed-drain:partitionedIngestMixed:putWorkload` | `5218.674 ops/s` | `4962.621 ops/s` | `-4.91%` | `warning` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed` | `207684.917 ops/s` | `195159.499 ops/s` | `-6.03%` | `warning` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:getWorkload` | `205044.275 ops/s` | `192675.110 ops/s` | `-6.03%` | `warning` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:putWorkload` | `2640.642 ops/s` | `2484.389 ops/s` | `-5.92%` | `warning` |
| `segment-index-persisted-mutation:deleteAsyncJoin` | `2160.231 ops/s` | `2289.976 ops/s` | `+6.01%` | `better` |
| `segment-index-persisted-mutation:deleteAsyncJoin:diag_directoryCountDelta` | `0.000 ops/s` | `-` | `-` | `removed` |
| `segment-index-persisted-mutation:deleteAsyncJoin:diag_fileBytesDelta` | `54491.914 ops/s` | `-` | `-` | `removed` |
| `segment-index-persisted-mutation:deleteAsyncJoin:diag_fileCountDelta` | `3.999 ops/s` | `-` | `-` | `removed` |
| `segment-index-persisted-mutation:deleteSync` | `2479.974 ops/s` | `2523.016 ops/s` | `+1.74%` | `neutral` |
| `segment-index-persisted-mutation:deleteSync:diag_directoryCountDelta` | `0.000 ops/s` | `-` | `-` | `removed` |
| `segment-index-persisted-mutation:deleteSync:diag_fileBytesDelta` | `58277.041 ops/s` | `-` | `-` | `removed` |
| `segment-index-persisted-mutation:deleteSync:diag_fileCountDelta` | `3.332 ops/s` | `-` | `-` | `removed` |
| `segment-index-persisted-mutation:putAsyncJoin` | `2215.411 ops/s` | `2221.799 ops/s` | `+0.29%` | `neutral` |
| `segment-index-persisted-mutation:putAsyncJoin:diag_directoryCountDelta` | `0.000 ops/s` | `-` | `-` | `removed` |
| `segment-index-persisted-mutation:putAsyncJoin:diag_fileBytesDelta` | `230876.647 ops/s` | `-` | `-` | `removed` |
| `segment-index-persisted-mutation:putAsyncJoin:diag_fileCountDelta` | `3.999 ops/s` | `-` | `-` | `removed` |
| `segment-index-persisted-mutation:putSync` | `2514.868 ops/s` | `2524.750 ops/s` | `+0.39%` | `neutral` |
| `segment-index-persisted-mutation:putSync:diag_directoryCountDelta` | `0.000 ops/s` | `-` | `-` | `removed` |
| `segment-index-persisted-mutation:putSync:diag_fileBytesDelta` | `258610.600 ops/s` | `-` | `-` | `removed` |
| `segment-index-persisted-mutation:putSync:diag_fileCountDelta` | `2.982 ops/s` | `-` | `-` | `removed` |
| `single-chunk-entry-write:writeEntrySteadyState` | `7932313.730 ops/s` | `-` | `-` | `removed` |
| `sorted-data-diff-key-read:readNextKey` | `6487103.344 ops/s` | `-` | `-` | `removed` |
