# Benchmark Comparison

- Profile: `segment-index-pr-smoke`
- Baseline SHA: `4d49a34ae20bb8595d3ac3657abe6c2cde168e52`
- Candidate SHA: `5765b56c3a8fa0c525ee29e4ca0a5c98d9b4ee42`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `segment-index-get-multisegment-hot:getHitAsyncJoin` | `87.894 ops/s` | `91.852 ops/s` | `+4.50%` | `better` |
| `segment-index-get-multisegment-hot:getHitSync` | `83.475 ops/s` | `90.309 ops/s` | `+8.19%` | `better` |
| `segment-index-get-multisegment-hot:getMissAsyncJoin` | `166671.207 ops/s` | `174193.944 ops/s` | `+4.51%` | `better` |
| `segment-index-get-multisegment-hot:getMissSync` | `3818353.259 ops/s` | `3953093.162 ops/s` | `+3.53%` | `better` |
| `segment-index-get-overlay:getHitAsyncJoin` | `163079.763 ops/s` | `171836.383 ops/s` | `+5.37%` | `better` |
| `segment-index-get-overlay:getHitSync` | `4181836.832 ops/s` | `4091655.601 ops/s` | `-2.16%` | `neutral` |
| `segment-index-get-overlay:getMissAsyncJoin` | `169682.062 ops/s` | `177120.111 ops/s` | `+4.38%` | `better` |
| `segment-index-get-overlay:getMissSync` | `4051237.012 ops/s` | `3992293.918 ops/s` | `-1.45%` | `neutral` |
| `segment-index-get-persisted:getHitAsyncJoin` | `56573.614 ops/s` | `61768.151 ops/s` | `+9.18%` | `better` |
| `segment-index-get-persisted:getHitSync` | `99751.629 ops/s` | `104939.616 ops/s` | `+5.20%` | `better` |
| `segment-index-get-persisted:getMissAsyncJoin` | `164694.248 ops/s` | `166389.888 ops/s` | `+1.03%` | `neutral` |
| `segment-index-get-persisted:getMissSync` | `4019268.170 ops/s` | `3750229.209 ops/s` | `-6.69%` | `warning` |
| `segment-index-hot-partition-put:putHotPartition` | `2983298.781 ops/s` | `3095697.252 ops/s` | `+3.77%` | `better` |
| `segment-index-hot-partition-put:putThenGetHotPartition` | `1626220.014 ops/s` | `1696552.912 ops/s` | `+4.32%` | `better` |
| `segment-index-mixed-drain:partitionedIngestMixed` | `464940.491 ops/s` | `446346.967 ops/s` | `-4.00%` | `warning` |
| `segment-index-mixed-drain:partitionedIngestMixed:getWorkload` | `459664.632 ops/s` | `441118.032 ops/s` | `-4.03%` | `warning` |
| `segment-index-mixed-drain:partitionedIngestMixed:putWorkload` | `5275.859 ops/s` | `5228.935 ops/s` | `-0.89%` | `neutral` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed` | `200163.770 ops/s` | `190079.393 ops/s` | `-5.04%` | `warning` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:getWorkload` | `197616.070 ops/s` | `187694.918 ops/s` | `-5.02%` | `warning` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:putWorkload` | `2547.700 ops/s` | `2384.475 ops/s` | `-6.41%` | `warning` |
| `segment-index-persisted-mutation:deleteAsyncJoin` | `2184.247 ops/s` | `1682.681 ops/s` | `-22.96%` | `worse` |
| `segment-index-persisted-mutation:deleteAsyncJoin:diag_directoryCountDelta` | `0.000 ops/s` | `-` | `-` | `removed` |
| `segment-index-persisted-mutation:deleteAsyncJoin:diag_fileBytesDelta` | `55186.967 ops/s` | `-` | `-` | `removed` |
| `segment-index-persisted-mutation:deleteAsyncJoin:diag_fileCountDelta` | `3.999 ops/s` | `-` | `-` | `removed` |
| `segment-index-persisted-mutation:deleteSync` | `2482.403 ops/s` | `1792.196 ops/s` | `-27.80%` | `worse` |
| `segment-index-persisted-mutation:deleteSync:diag_directoryCountDelta` | `0.000 ops/s` | `-` | `-` | `removed` |
| `segment-index-persisted-mutation:deleteSync:diag_fileBytesDelta` | `55701.128 ops/s` | `-` | `-` | `removed` |
| `segment-index-persisted-mutation:deleteSync:diag_fileCountDelta` | `2.983 ops/s` | `-` | `-` | `removed` |
| `segment-index-persisted-mutation:putAsyncJoin` | `2235.485 ops/s` | `1711.417 ops/s` | `-23.44%` | `worse` |
| `segment-index-persisted-mutation:putAsyncJoin:diag_directoryCountDelta` | `0.000 ops/s` | `-` | `-` | `removed` |
| `segment-index-persisted-mutation:putAsyncJoin:diag_fileBytesDelta` | `230018.697 ops/s` | `-` | `-` | `removed` |
| `segment-index-persisted-mutation:putAsyncJoin:diag_fileCountDelta` | `3.666 ops/s` | `-` | `-` | `removed` |
| `segment-index-persisted-mutation:putSync` | `2463.563 ops/s` | `1667.737 ops/s` | `-32.30%` | `worse` |
| `segment-index-persisted-mutation:putSync:diag_directoryCountDelta` | `0.000 ops/s` | `-` | `-` | `removed` |
| `segment-index-persisted-mutation:putSync:diag_fileBytesDelta` | `253980.576 ops/s` | `-` | `-` | `removed` |
| `segment-index-persisted-mutation:putSync:diag_fileCountDelta` | `2.999 ops/s` | `-` | `-` | `removed` |
| `single-chunk-entry-write:writeEntrySteadyState` | `7891969.477 ops/s` | `-` | `-` | `removed` |
| `sorted-data-diff-key-read:readNextKey` | `6608303.151 ops/s` | `-` | `-` | `removed` |
