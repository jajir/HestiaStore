# Benchmark Comparison

- Profile: `segment-index-pr-smoke`
- Baseline SHA: `4d49a34ae20bb8595d3ac3657abe6c2cde168e52`
- Candidate SHA: `fe9d2ad38cc8d16b0023cbd29a171712a3c12d0c`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `segment-index-get-multisegment-hot:getHitAsyncJoin` | `87.894 ops/s` | `82.864 ops/s` | `-5.72%` | `warning` |
| `segment-index-get-multisegment-hot:getHitSync` | `83.475 ops/s` | `86.223 ops/s` | `+3.29%` | `better` |
| `segment-index-get-multisegment-hot:getMissAsyncJoin` | `166671.207 ops/s` | `170042.362 ops/s` | `+2.02%` | `neutral` |
| `segment-index-get-multisegment-hot:getMissSync` | `3818353.259 ops/s` | `3901032.047 ops/s` | `+2.17%` | `neutral` |
| `segment-index-get-overlay:getHitAsyncJoin` | `163079.763 ops/s` | `163032.930 ops/s` | `-0.03%` | `neutral` |
| `segment-index-get-overlay:getHitSync` | `4181836.832 ops/s` | `4410268.343 ops/s` | `+5.46%` | `better` |
| `segment-index-get-overlay:getMissAsyncJoin` | `169682.062 ops/s` | `172227.552 ops/s` | `+1.50%` | `neutral` |
| `segment-index-get-overlay:getMissSync` | `4051237.012 ops/s` | `4135435.545 ops/s` | `+2.08%` | `neutral` |
| `segment-index-get-persisted:getHitAsyncJoin` | `56573.614 ops/s` | `56259.277 ops/s` | `-0.56%` | `neutral` |
| `segment-index-get-persisted:getHitSync` | `99751.629 ops/s` | `109899.778 ops/s` | `+10.17%` | `better` |
| `segment-index-get-persisted:getMissAsyncJoin` | `164694.248 ops/s` | `172377.503 ops/s` | `+4.67%` | `better` |
| `segment-index-get-persisted:getMissSync` | `4019268.170 ops/s` | `3992837.174 ops/s` | `-0.66%` | `neutral` |
| `segment-index-hot-partition-put:putHotPartition` | `2983298.781 ops/s` | `2951765.882 ops/s` | `-1.06%` | `neutral` |
| `segment-index-hot-partition-put:putThenGetHotPartition` | `1626220.014 ops/s` | `1666361.567 ops/s` | `+2.47%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed` | `464940.491 ops/s` | `451289.862 ops/s` | `-2.94%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed:getWorkload` | `459664.632 ops/s` | `445958.061 ops/s` | `-2.98%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed:putWorkload` | `5275.859 ops/s` | `5331.801 ops/s` | `+1.06%` | `neutral` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed` | `200163.770 ops/s` | `192063.161 ops/s` | `-4.05%` | `warning` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:getWorkload` | `197616.070 ops/s` | `189500.365 ops/s` | `-4.11%` | `warning` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:putWorkload` | `2547.700 ops/s` | `2562.796 ops/s` | `+0.59%` | `neutral` |
| `segment-index-persisted-mutation:deleteAsyncJoin` | `2184.247 ops/s` | `2324.608 ops/s` | `+6.43%` | `better` |
| `segment-index-persisted-mutation:deleteAsyncJoin:diag_directoryCountDelta` | `0.000 ops/s` | `-` | `-` | `removed` |
| `segment-index-persisted-mutation:deleteAsyncJoin:diag_fileBytesDelta` | `55186.967 ops/s` | `-` | `-` | `removed` |
| `segment-index-persisted-mutation:deleteAsyncJoin:diag_fileCountDelta` | `3.999 ops/s` | `-` | `-` | `removed` |
| `segment-index-persisted-mutation:deleteSync` | `2482.403 ops/s` | `2631.502 ops/s` | `+6.01%` | `better` |
| `segment-index-persisted-mutation:deleteSync:diag_directoryCountDelta` | `0.000 ops/s` | `-` | `-` | `removed` |
| `segment-index-persisted-mutation:deleteSync:diag_fileBytesDelta` | `55701.128 ops/s` | `-` | `-` | `removed` |
| `segment-index-persisted-mutation:deleteSync:diag_fileCountDelta` | `2.983 ops/s` | `-` | `-` | `removed` |
| `segment-index-persisted-mutation:putAsyncJoin` | `2235.485 ops/s` | `2329.543 ops/s` | `+4.21%` | `better` |
| `segment-index-persisted-mutation:putAsyncJoin:diag_directoryCountDelta` | `0.000 ops/s` | `-` | `-` | `removed` |
| `segment-index-persisted-mutation:putAsyncJoin:diag_fileBytesDelta` | `230018.697 ops/s` | `-` | `-` | `removed` |
| `segment-index-persisted-mutation:putAsyncJoin:diag_fileCountDelta` | `3.666 ops/s` | `-` | `-` | `removed` |
| `segment-index-persisted-mutation:putSync` | `2463.563 ops/s` | `2600.753 ops/s` | `+5.57%` | `better` |
| `segment-index-persisted-mutation:putSync:diag_directoryCountDelta` | `0.000 ops/s` | `-` | `-` | `removed` |
| `segment-index-persisted-mutation:putSync:diag_fileBytesDelta` | `253980.576 ops/s` | `-` | `-` | `removed` |
| `segment-index-persisted-mutation:putSync:diag_fileCountDelta` | `2.999 ops/s` | `-` | `-` | `removed` |
| `single-chunk-entry-write:writeEntrySteadyState` | `7891969.477 ops/s` | `-` | `-` | `removed` |
| `sorted-data-diff-key-read:readNextKey` | `6608303.151 ops/s` | `-` | `-` | `removed` |
