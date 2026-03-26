# Benchmark Comparison

- Profile: `segment-index-pr-smoke`
- Baseline SHA: `e72f4974ea597aa48d2725b131d13e39bcfa6872`
- Candidate SHA: `27e2bececc6c83f241b980a6b2415f9a7c247c09`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `segment-index-get-multisegment-hot:getHitAsyncJoin` | `97.043 ops/s` | `96.127 ops/s` | `-0.94%` | `neutral` |
| `segment-index-get-multisegment-hot:getHitSync` | `84.004 ops/s` | `97.307 ops/s` | `+15.84%` | `better` |
| `segment-index-get-multisegment-hot:getMissAsyncJoin` | `165098.087 ops/s` | `176463.159 ops/s` | `+6.88%` | `better` |
| `segment-index-get-multisegment-hot:getMissSync` | `3826025.739 ops/s` | `3878183.204 ops/s` | `+1.36%` | `neutral` |
| `segment-index-get-overlay:getHitAsyncJoin` | `155664.746 ops/s` | `173410.194 ops/s` | `+11.40%` | `better` |
| `segment-index-get-overlay:getHitSync` | `4213435.612 ops/s` | `4297440.769 ops/s` | `+1.99%` | `neutral` |
| `segment-index-get-overlay:getMissAsyncJoin` | `156455.503 ops/s` | `177268.794 ops/s` | `+13.30%` | `better` |
| `segment-index-get-overlay:getMissSync` | `4124415.193 ops/s` | `3904450.747 ops/s` | `-5.33%` | `warning` |
| `segment-index-get-persisted:getHitAsyncJoin` | `55789.753 ops/s` | `49181.658 ops/s` | `-11.84%` | `worse` |
| `segment-index-get-persisted:getHitSync` | `104396.145 ops/s` | `105145.992 ops/s` | `+0.72%` | `neutral` |
| `segment-index-get-persisted:getMissAsyncJoin` | `165994.696 ops/s` | `168369.530 ops/s` | `+1.43%` | `neutral` |
| `segment-index-get-persisted:getMissSync` | `3940414.940 ops/s` | `3915485.941 ops/s` | `-0.63%` | `neutral` |
| `segment-index-hot-partition-put:putHotPartition` | `3124721.140 ops/s` | `2826199.709 ops/s` | `-9.55%` | `worse` |
| `segment-index-hot-partition-put:putThenGetHotPartition` | `1717560.089 ops/s` | `1704965.919 ops/s` | `-0.73%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed` | `455608.507 ops/s` | `444210.684 ops/s` | `-2.50%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed:getWorkload` | `450389.833 ops/s` | `438919.858 ops/s` | `-2.55%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed:putWorkload` | `5218.674 ops/s` | `5290.826 ops/s` | `+1.38%` | `neutral` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed` | `207684.917 ops/s` | `163928.500 ops/s` | `-21.07%` | `worse` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:getWorkload` | `205044.275 ops/s` | `160984.897 ops/s` | `-21.49%` | `worse` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:putWorkload` | `2640.642 ops/s` | `2943.603 ops/s` | `+11.47%` | `better` |
| `segment-index-persisted-mutation:deleteAsyncJoin` | `2160.231 ops/s` | `1924.156 ops/s` | `-10.93%` | `worse` |
| `segment-index-persisted-mutation:deleteAsyncJoin:diag_directoryCountDelta` | `0.000 ops/s` | `-` | `-` | `removed` |
| `segment-index-persisted-mutation:deleteAsyncJoin:diag_fileBytesDelta` | `54491.914 ops/s` | `-` | `-` | `removed` |
| `segment-index-persisted-mutation:deleteAsyncJoin:diag_fileCountDelta` | `3.999 ops/s` | `-` | `-` | `removed` |
| `segment-index-persisted-mutation:deleteSync` | `2479.974 ops/s` | `2332.225 ops/s` | `-5.96%` | `warning` |
| `segment-index-persisted-mutation:deleteSync:diag_directoryCountDelta` | `0.000 ops/s` | `-` | `-` | `removed` |
| `segment-index-persisted-mutation:deleteSync:diag_fileBytesDelta` | `58277.041 ops/s` | `-` | `-` | `removed` |
| `segment-index-persisted-mutation:deleteSync:diag_fileCountDelta` | `3.332 ops/s` | `-` | `-` | `removed` |
| `segment-index-persisted-mutation:putAsyncJoin` | `2215.411 ops/s` | `1900.336 ops/s` | `-14.22%` | `worse` |
| `segment-index-persisted-mutation:putAsyncJoin:diag_directoryCountDelta` | `0.000 ops/s` | `-` | `-` | `removed` |
| `segment-index-persisted-mutation:putAsyncJoin:diag_fileBytesDelta` | `230876.647 ops/s` | `-` | `-` | `removed` |
| `segment-index-persisted-mutation:putAsyncJoin:diag_fileCountDelta` | `3.999 ops/s` | `-` | `-` | `removed` |
| `segment-index-persisted-mutation:putSync` | `2514.868 ops/s` | `2083.279 ops/s` | `-17.16%` | `worse` |
| `segment-index-persisted-mutation:putSync:diag_directoryCountDelta` | `0.000 ops/s` | `-` | `-` | `removed` |
| `segment-index-persisted-mutation:putSync:diag_fileBytesDelta` | `258610.600 ops/s` | `-` | `-` | `removed` |
| `segment-index-persisted-mutation:putSync:diag_fileCountDelta` | `2.982 ops/s` | `-` | `-` | `removed` |
| `single-chunk-entry-write:writeEntrySteadyState` | `7932313.730 ops/s` | `-` | `-` | `removed` |
| `sorted-data-diff-key-read:readNextKey` | `6487103.344 ops/s` | `-` | `-` | `removed` |
