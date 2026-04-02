# Benchmark Comparison

- Profile: `segment-index-pr-smoke`
- Baseline SHA: `e72f4974ea597aa48d2725b131d13e39bcfa6872`
- Candidate SHA: `20fa8f143cde842427db8056ecec8e9e5b7e1124`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `segment-index-get-multisegment-hot:getHitAsyncJoin` | `97.043 ops/s` | `96.989 ops/s` | `-0.06%` | `neutral` |
| `segment-index-get-multisegment-hot:getHitSync` | `84.004 ops/s` | `82.180 ops/s` | `-2.17%` | `neutral` |
| `segment-index-get-multisegment-hot:getMissAsyncJoin` | `165098.087 ops/s` | `186382.146 ops/s` | `+12.89%` | `better` |
| `segment-index-get-multisegment-hot:getMissSync` | `3826025.739 ops/s` | `3917887.681 ops/s` | `+2.40%` | `neutral` |
| `segment-index-get-overlay:getHitAsyncJoin` | `155664.746 ops/s` | `185883.422 ops/s` | `+19.41%` | `better` |
| `segment-index-get-overlay:getHitSync` | `4213435.612 ops/s` | `4202627.709 ops/s` | `-0.26%` | `neutral` |
| `segment-index-get-overlay:getMissAsyncJoin` | `156455.503 ops/s` | `185220.250 ops/s` | `+18.39%` | `better` |
| `segment-index-get-overlay:getMissSync` | `4124415.193 ops/s` | `3856459.533 ops/s` | `-6.50%` | `warning` |
| `segment-index-get-persisted:getHitAsyncJoin` | `55789.753 ops/s` | `53282.692 ops/s` | `-4.49%` | `warning` |
| `segment-index-get-persisted:getHitSync` | `104396.145 ops/s` | `119179.257 ops/s` | `+14.16%` | `better` |
| `segment-index-get-persisted:getMissAsyncJoin` | `165994.696 ops/s` | `183771.566 ops/s` | `+10.71%` | `better` |
| `segment-index-get-persisted:getMissSync` | `3940414.940 ops/s` | `3902570.642 ops/s` | `-0.96%` | `neutral` |
| `segment-index-hot-partition-put:putHotPartition` | `3124721.140 ops/s` | `3005609.649 ops/s` | `-3.81%` | `warning` |
| `segment-index-hot-partition-put:putThenGetHotPartition` | `1717560.089 ops/s` | `1618554.971 ops/s` | `-5.76%` | `warning` |
| `segment-index-mixed-drain:partitionedIngestMixed` | `455608.507 ops/s` | `406006.099 ops/s` | `-10.89%` | `worse` |
| `segment-index-mixed-drain:partitionedIngestMixed:getWorkload` | `450389.833 ops/s` | `400925.878 ops/s` | `-10.98%` | `worse` |
| `segment-index-mixed-drain:partitionedIngestMixed:putWorkload` | `5218.674 ops/s` | `5080.221 ops/s` | `-2.65%` | `neutral` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed` | `207684.917 ops/s` | `-` | `-` | `removed` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:getWorkload` | `205044.275 ops/s` | `-` | `-` | `removed` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:putWorkload` | `2640.642 ops/s` | `-` | `-` | `removed` |
| `segment-index-persisted-mutation:deleteAsyncJoin` | `2160.231 ops/s` | `2617.500 ops/s` | `+21.17%` | `better` |
| `segment-index-persisted-mutation:deleteAsyncJoin:diag_directoryCountDelta` | `0.000 ops/s` | `-` | `-` | `removed` |
| `segment-index-persisted-mutation:deleteAsyncJoin:diag_fileBytesDelta` | `54491.914 ops/s` | `-` | `-` | `removed` |
| `segment-index-persisted-mutation:deleteAsyncJoin:diag_fileCountDelta` | `3.999 ops/s` | `-` | `-` | `removed` |
| `segment-index-persisted-mutation:deleteSync` | `2479.974 ops/s` | `3262.847 ops/s` | `+31.57%` | `better` |
| `segment-index-persisted-mutation:deleteSync:diag_directoryCountDelta` | `0.000 ops/s` | `-` | `-` | `removed` |
| `segment-index-persisted-mutation:deleteSync:diag_fileBytesDelta` | `58277.041 ops/s` | `-` | `-` | `removed` |
| `segment-index-persisted-mutation:deleteSync:diag_fileCountDelta` | `3.332 ops/s` | `-` | `-` | `removed` |
| `segment-index-persisted-mutation:putAsyncJoin` | `2215.411 ops/s` | `2676.707 ops/s` | `+20.82%` | `better` |
| `segment-index-persisted-mutation:putAsyncJoin:diag_directoryCountDelta` | `0.000 ops/s` | `-` | `-` | `removed` |
| `segment-index-persisted-mutation:putAsyncJoin:diag_fileBytesDelta` | `230876.647 ops/s` | `-` | `-` | `removed` |
| `segment-index-persisted-mutation:putAsyncJoin:diag_fileCountDelta` | `3.999 ops/s` | `-` | `-` | `removed` |
| `segment-index-persisted-mutation:putSync` | `2514.868 ops/s` | `2993.614 ops/s` | `+19.04%` | `better` |
| `segment-index-persisted-mutation:putSync:diag_directoryCountDelta` | `0.000 ops/s` | `-` | `-` | `removed` |
| `segment-index-persisted-mutation:putSync:diag_fileBytesDelta` | `258610.600 ops/s` | `-` | `-` | `removed` |
| `segment-index-persisted-mutation:putSync:diag_fileCountDelta` | `2.982 ops/s` | `-` | `-` | `removed` |
| `single-chunk-entry-write:writeEntrySteadyState` | `7932313.730 ops/s` | `-` | `-` | `removed` |
| `sorted-data-diff-key-read:readNextKey` | `6487103.344 ops/s` | `-` | `-` | `removed` |
