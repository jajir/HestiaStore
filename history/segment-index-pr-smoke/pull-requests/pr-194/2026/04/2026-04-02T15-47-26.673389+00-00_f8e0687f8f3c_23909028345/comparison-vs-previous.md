# Benchmark Comparison

- Profile: `segment-index-pr-smoke`
- Baseline SHA: `e72f4974ea597aa48d2725b131d13e39bcfa6872`
- Candidate SHA: `f8e0687f8f3c5d8dd2edb27b63c0164da514395e`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `segment-index-get-multisegment-hot:getHitAsyncJoin` | `97.043 ops/s` | `84.743 ops/s` | `-12.68%` | `worse` |
| `segment-index-get-multisegment-hot:getHitSync` | `84.004 ops/s` | `86.879 ops/s` | `+3.42%` | `better` |
| `segment-index-get-multisegment-hot:getMissAsyncJoin` | `165098.087 ops/s` | `184787.534 ops/s` | `+11.93%` | `better` |
| `segment-index-get-multisegment-hot:getMissSync` | `3826025.739 ops/s` | `3663856.714 ops/s` | `-4.24%` | `warning` |
| `segment-index-get-overlay:getHitAsyncJoin` | `155664.746 ops/s` | `170581.220 ops/s` | `+9.58%` | `better` |
| `segment-index-get-overlay:getHitSync` | `4213435.612 ops/s` | `4228237.000 ops/s` | `+0.35%` | `neutral` |
| `segment-index-get-overlay:getMissAsyncJoin` | `156455.503 ops/s` | `179374.289 ops/s` | `+14.65%` | `better` |
| `segment-index-get-overlay:getMissSync` | `4124415.193 ops/s` | `4098856.663 ops/s` | `-0.62%` | `neutral` |
| `segment-index-get-persisted:getHitAsyncJoin` | `55789.753 ops/s` | `60859.397 ops/s` | `+9.09%` | `better` |
| `segment-index-get-persisted:getHitSync` | `104396.145 ops/s` | `121559.709 ops/s` | `+16.44%` | `better` |
| `segment-index-get-persisted:getMissAsyncJoin` | `165994.696 ops/s` | `168460.332 ops/s` | `+1.49%` | `neutral` |
| `segment-index-get-persisted:getMissSync` | `3940414.940 ops/s` | `4117865.910 ops/s` | `+4.50%` | `better` |
| `segment-index-hot-partition-put:putHotPartition` | `3124721.140 ops/s` | `3020564.454 ops/s` | `-3.33%` | `warning` |
| `segment-index-hot-partition-put:putThenGetHotPartition` | `1717560.089 ops/s` | `1646047.146 ops/s` | `-4.16%` | `warning` |
| `segment-index-mixed-drain:partitionedIngestMixed` | `455608.507 ops/s` | `420498.195 ops/s` | `-7.71%` | `worse` |
| `segment-index-mixed-drain:partitionedIngestMixed:getWorkload` | `450389.833 ops/s` | `415283.334 ops/s` | `-7.79%` | `worse` |
| `segment-index-mixed-drain:partitionedIngestMixed:putWorkload` | `5218.674 ops/s` | `5214.860 ops/s` | `-0.07%` | `neutral` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed` | `207684.917 ops/s` | `161901.554 ops/s` | `-22.04%` | `worse` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:getWorkload` | `205044.275 ops/s` | `159350.330 ops/s` | `-22.28%` | `worse` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:putWorkload` | `2640.642 ops/s` | `2551.225 ops/s` | `-3.39%` | `warning` |
| `segment-index-persisted-mutation:deleteAsyncJoin` | `2160.231 ops/s` | `2063.553 ops/s` | `-4.48%` | `warning` |
| `segment-index-persisted-mutation:deleteAsyncJoin:diag_directoryCountDelta` | `0.000 ops/s` | `-` | `-` | `removed` |
| `segment-index-persisted-mutation:deleteAsyncJoin:diag_fileBytesDelta` | `54491.914 ops/s` | `-` | `-` | `removed` |
| `segment-index-persisted-mutation:deleteAsyncJoin:diag_fileCountDelta` | `3.999 ops/s` | `-` | `-` | `removed` |
| `segment-index-persisted-mutation:deleteSync` | `2479.974 ops/s` | `2402.683 ops/s` | `-3.12%` | `warning` |
| `segment-index-persisted-mutation:deleteSync:diag_directoryCountDelta` | `0.000 ops/s` | `-` | `-` | `removed` |
| `segment-index-persisted-mutation:deleteSync:diag_fileBytesDelta` | `58277.041 ops/s` | `-` | `-` | `removed` |
| `segment-index-persisted-mutation:deleteSync:diag_fileCountDelta` | `3.332 ops/s` | `-` | `-` | `removed` |
| `segment-index-persisted-mutation:putAsyncJoin` | `2215.411 ops/s` | `2063.200 ops/s` | `-6.87%` | `warning` |
| `segment-index-persisted-mutation:putAsyncJoin:diag_directoryCountDelta` | `0.000 ops/s` | `-` | `-` | `removed` |
| `segment-index-persisted-mutation:putAsyncJoin:diag_fileBytesDelta` | `230876.647 ops/s` | `-` | `-` | `removed` |
| `segment-index-persisted-mutation:putAsyncJoin:diag_fileCountDelta` | `3.999 ops/s` | `-` | `-` | `removed` |
| `segment-index-persisted-mutation:putSync` | `2514.868 ops/s` | `2216.584 ops/s` | `-11.86%` | `worse` |
| `segment-index-persisted-mutation:putSync:diag_directoryCountDelta` | `0.000 ops/s` | `-` | `-` | `removed` |
| `segment-index-persisted-mutation:putSync:diag_fileBytesDelta` | `258610.600 ops/s` | `-` | `-` | `removed` |
| `segment-index-persisted-mutation:putSync:diag_fileCountDelta` | `2.982 ops/s` | `-` | `-` | `removed` |
| `single-chunk-entry-write:writeEntrySteadyState` | `7932313.730 ops/s` | `-` | `-` | `removed` |
| `sorted-data-diff-key-read:readNextKey` | `6487103.344 ops/s` | `-` | `-` | `removed` |
