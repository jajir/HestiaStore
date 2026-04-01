# Benchmark Comparison

- Profile: `segment-index-pr-smoke`
- Baseline SHA: `e72f4974ea597aa48d2725b131d13e39bcfa6872`
- Candidate SHA: `184213bc0aceb70f24eaf2bdb903c98215beaf74`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `segment-index-get-multisegment-hot:getHitAsyncJoin` | `97.043 ops/s` | `90.180 ops/s` | `-7.07%` | `worse` |
| `segment-index-get-multisegment-hot:getHitSync` | `84.004 ops/s` | `94.406 ops/s` | `+12.38%` | `better` |
| `segment-index-get-multisegment-hot:getMissAsyncJoin` | `165098.087 ops/s` | `184572.026 ops/s` | `+11.80%` | `better` |
| `segment-index-get-multisegment-hot:getMissSync` | `3826025.739 ops/s` | `3810084.551 ops/s` | `-0.42%` | `neutral` |
| `segment-index-get-overlay:getHitAsyncJoin` | `155664.746 ops/s` | `174002.663 ops/s` | `+11.78%` | `better` |
| `segment-index-get-overlay:getHitSync` | `4213435.612 ops/s` | `4103903.501 ops/s` | `-2.60%` | `neutral` |
| `segment-index-get-overlay:getMissAsyncJoin` | `156455.503 ops/s` | `177437.063 ops/s` | `+13.41%` | `better` |
| `segment-index-get-overlay:getMissSync` | `4124415.193 ops/s` | `3905406.685 ops/s` | `-5.31%` | `warning` |
| `segment-index-get-persisted:getHitAsyncJoin` | `55789.753 ops/s` | `66073.314 ops/s` | `+18.43%` | `better` |
| `segment-index-get-persisted:getHitSync` | `104396.145 ops/s` | `123490.309 ops/s` | `+18.29%` | `better` |
| `segment-index-get-persisted:getMissAsyncJoin` | `165994.696 ops/s` | `170911.015 ops/s` | `+2.96%` | `neutral` |
| `segment-index-get-persisted:getMissSync` | `3940414.940 ops/s` | `3982416.578 ops/s` | `+1.07%` | `neutral` |
| `segment-index-hot-partition-put:putHotPartition` | `3124721.140 ops/s` | `3031708.365 ops/s` | `-2.98%` | `neutral` |
| `segment-index-hot-partition-put:putThenGetHotPartition` | `1717560.089 ops/s` | `1675926.015 ops/s` | `-2.42%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed` | `455608.507 ops/s` | `433597.580 ops/s` | `-4.83%` | `warning` |
| `segment-index-mixed-drain:partitionedIngestMixed:getWorkload` | `450389.833 ops/s` | `428446.988 ops/s` | `-4.87%` | `warning` |
| `segment-index-mixed-drain:partitionedIngestMixed:putWorkload` | `5218.674 ops/s` | `5150.591 ops/s` | `-1.30%` | `neutral` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed` | `207684.917 ops/s` | `189951.001 ops/s` | `-8.54%` | `worse` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:getWorkload` | `205044.275 ops/s` | `187454.570 ops/s` | `-8.58%` | `worse` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:putWorkload` | `2640.642 ops/s` | `2496.432 ops/s` | `-5.46%` | `warning` |
| `segment-index-persisted-mutation:deleteAsyncJoin` | `2160.231 ops/s` | `1424.353 ops/s` | `-34.06%` | `worse` |
| `segment-index-persisted-mutation:deleteAsyncJoin:diag_directoryCountDelta` | `0.000 ops/s` | `-` | `-` | `removed` |
| `segment-index-persisted-mutation:deleteAsyncJoin:diag_fileBytesDelta` | `54491.914 ops/s` | `-` | `-` | `removed` |
| `segment-index-persisted-mutation:deleteAsyncJoin:diag_fileCountDelta` | `3.999 ops/s` | `-` | `-` | `removed` |
| `segment-index-persisted-mutation:deleteSync` | `2479.974 ops/s` | `1458.154 ops/s` | `-41.20%` | `worse` |
| `segment-index-persisted-mutation:deleteSync:diag_directoryCountDelta` | `0.000 ops/s` | `-` | `-` | `removed` |
| `segment-index-persisted-mutation:deleteSync:diag_fileBytesDelta` | `58277.041 ops/s` | `-` | `-` | `removed` |
| `segment-index-persisted-mutation:deleteSync:diag_fileCountDelta` | `3.332 ops/s` | `-` | `-` | `removed` |
| `segment-index-persisted-mutation:putAsyncJoin` | `2215.411 ops/s` | `1291.102 ops/s` | `-41.72%` | `worse` |
| `segment-index-persisted-mutation:putAsyncJoin:diag_directoryCountDelta` | `0.000 ops/s` | `-` | `-` | `removed` |
| `segment-index-persisted-mutation:putAsyncJoin:diag_fileBytesDelta` | `230876.647 ops/s` | `-` | `-` | `removed` |
| `segment-index-persisted-mutation:putAsyncJoin:diag_fileCountDelta` | `3.999 ops/s` | `-` | `-` | `removed` |
| `segment-index-persisted-mutation:putSync` | `2514.868 ops/s` | `1331.368 ops/s` | `-47.06%` | `worse` |
| `segment-index-persisted-mutation:putSync:diag_directoryCountDelta` | `0.000 ops/s` | `-` | `-` | `removed` |
| `segment-index-persisted-mutation:putSync:diag_fileBytesDelta` | `258610.600 ops/s` | `-` | `-` | `removed` |
| `segment-index-persisted-mutation:putSync:diag_fileCountDelta` | `2.982 ops/s` | `-` | `-` | `removed` |
| `single-chunk-entry-write:writeEntrySteadyState` | `7932313.730 ops/s` | `-` | `-` | `removed` |
| `sorted-data-diff-key-read:readNextKey` | `6487103.344 ops/s` | `-` | `-` | `removed` |
