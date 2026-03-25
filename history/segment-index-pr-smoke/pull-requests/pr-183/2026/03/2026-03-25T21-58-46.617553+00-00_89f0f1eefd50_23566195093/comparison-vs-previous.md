# Benchmark Comparison

- Profile: `segment-index-pr-smoke`
- Baseline SHA: `4d49a34ae20bb8595d3ac3657abe6c2cde168e52`
- Candidate SHA: `89f0f1eefd50813f6848424281c6b8b3c09aa4c5`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `segment-index-get-multisegment-hot:getHitAsyncJoin` | `87.894 ops/s` | `93.975 ops/s` | `+6.92%` | `better` |
| `segment-index-get-multisegment-hot:getHitSync` | `83.475 ops/s` | `90.112 ops/s` | `+7.95%` | `better` |
| `segment-index-get-multisegment-hot:getMissAsyncJoin` | `166671.207 ops/s` | `175468.451 ops/s` | `+5.28%` | `better` |
| `segment-index-get-multisegment-hot:getMissSync` | `3818353.259 ops/s` | `3949403.787 ops/s` | `+3.43%` | `better` |
| `segment-index-get-overlay:getHitAsyncJoin` | `163079.763 ops/s` | `174405.689 ops/s` | `+6.95%` | `better` |
| `segment-index-get-overlay:getHitSync` | `4181836.832 ops/s` | `4299308.634 ops/s` | `+2.81%` | `neutral` |
| `segment-index-get-overlay:getMissAsyncJoin` | `169682.062 ops/s` | `175998.161 ops/s` | `+3.72%` | `better` |
| `segment-index-get-overlay:getMissSync` | `4051237.012 ops/s` | `4276973.376 ops/s` | `+5.57%` | `better` |
| `segment-index-get-persisted:getHitAsyncJoin` | `56573.614 ops/s` | `55694.826 ops/s` | `-1.55%` | `neutral` |
| `segment-index-get-persisted:getHitSync` | `99751.629 ops/s` | `96766.985 ops/s` | `-2.99%` | `neutral` |
| `segment-index-get-persisted:getMissAsyncJoin` | `164694.248 ops/s` | `175102.438 ops/s` | `+6.32%` | `better` |
| `segment-index-get-persisted:getMissSync` | `4019268.170 ops/s` | `3997293.935 ops/s` | `-0.55%` | `neutral` |
| `segment-index-hot-partition-put:putHotPartition` | `2983298.781 ops/s` | `3172633.443 ops/s` | `+6.35%` | `better` |
| `segment-index-hot-partition-put:putThenGetHotPartition` | `1626220.014 ops/s` | `1615949.855 ops/s` | `-0.63%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed` | `464940.491 ops/s` | `423869.194 ops/s` | `-8.83%` | `worse` |
| `segment-index-mixed-drain:partitionedIngestMixed:getWorkload` | `459664.632 ops/s` | `418569.053 ops/s` | `-8.94%` | `worse` |
| `segment-index-mixed-drain:partitionedIngestMixed:putWorkload` | `5275.859 ops/s` | `5300.141 ops/s` | `+0.46%` | `neutral` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed` | `200163.770 ops/s` | `196123.591 ops/s` | `-2.02%` | `neutral` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:getWorkload` | `197616.070 ops/s` | `193847.978 ops/s` | `-1.91%` | `neutral` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:putWorkload` | `2547.700 ops/s` | `2275.613 ops/s` | `-10.68%` | `worse` |
| `segment-index-persisted-mutation:deleteAsyncJoin` | `2184.247 ops/s` | `2147.591 ops/s` | `-1.68%` | `neutral` |
| `segment-index-persisted-mutation:deleteAsyncJoin:diag_directoryCountDelta` | `0.000 ops/s` | `-` | `-` | `removed` |
| `segment-index-persisted-mutation:deleteAsyncJoin:diag_fileBytesDelta` | `55186.967 ops/s` | `-` | `-` | `removed` |
| `segment-index-persisted-mutation:deleteAsyncJoin:diag_fileCountDelta` | `3.999 ops/s` | `-` | `-` | `removed` |
| `segment-index-persisted-mutation:deleteSync` | `2482.403 ops/s` | `2416.287 ops/s` | `-2.66%` | `neutral` |
| `segment-index-persisted-mutation:deleteSync:diag_directoryCountDelta` | `0.000 ops/s` | `-` | `-` | `removed` |
| `segment-index-persisted-mutation:deleteSync:diag_fileBytesDelta` | `55701.128 ops/s` | `-` | `-` | `removed` |
| `segment-index-persisted-mutation:deleteSync:diag_fileCountDelta` | `2.983 ops/s` | `-` | `-` | `removed` |
| `segment-index-persisted-mutation:putAsyncJoin` | `2235.485 ops/s` | `2144.663 ops/s` | `-4.06%` | `warning` |
| `segment-index-persisted-mutation:putAsyncJoin:diag_directoryCountDelta` | `0.000 ops/s` | `-` | `-` | `removed` |
| `segment-index-persisted-mutation:putAsyncJoin:diag_fileBytesDelta` | `230018.697 ops/s` | `-` | `-` | `removed` |
| `segment-index-persisted-mutation:putAsyncJoin:diag_fileCountDelta` | `3.666 ops/s` | `-` | `-` | `removed` |
| `segment-index-persisted-mutation:putSync` | `2463.563 ops/s` | `2350.619 ops/s` | `-4.58%` | `warning` |
| `segment-index-persisted-mutation:putSync:diag_directoryCountDelta` | `0.000 ops/s` | `-` | `-` | `removed` |
| `segment-index-persisted-mutation:putSync:diag_fileBytesDelta` | `253980.576 ops/s` | `-` | `-` | `removed` |
| `segment-index-persisted-mutation:putSync:diag_fileCountDelta` | `2.999 ops/s` | `-` | `-` | `removed` |
| `single-chunk-entry-write:writeEntrySteadyState` | `7891969.477 ops/s` | `-` | `-` | `removed` |
| `sorted-data-diff-key-read:readNextKey` | `6608303.151 ops/s` | `-` | `-` | `removed` |
