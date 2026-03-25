# Benchmark Comparison

- Profile: `segment-index-pr-smoke`
- Baseline SHA: `4d49a34ae20bb8595d3ac3657abe6c2cde168e52`
- Candidate SHA: `9846d5393114884362300d1f0a94338dbe97cc9e`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `segment-index-get-multisegment-hot:getHitAsyncJoin` | `87.894 ops/s` | `97.483 ops/s` | `+10.91%` | `better` |
| `segment-index-get-multisegment-hot:getHitSync` | `83.475 ops/s` | `93.440 ops/s` | `+11.94%` | `better` |
| `segment-index-get-multisegment-hot:getMissAsyncJoin` | `166671.207 ops/s` | `174598.826 ops/s` | `+4.76%` | `better` |
| `segment-index-get-multisegment-hot:getMissSync` | `3818353.259 ops/s` | `3924219.240 ops/s` | `+2.77%` | `neutral` |
| `segment-index-get-overlay:getHitAsyncJoin` | `163079.763 ops/s` | `170609.577 ops/s` | `+4.62%` | `better` |
| `segment-index-get-overlay:getHitSync` | `4181836.832 ops/s` | `4147296.432 ops/s` | `-0.83%` | `neutral` |
| `segment-index-get-overlay:getMissAsyncJoin` | `169682.062 ops/s` | `176562.565 ops/s` | `+4.05%` | `better` |
| `segment-index-get-overlay:getMissSync` | `4051237.012 ops/s` | `4004741.268 ops/s` | `-1.15%` | `neutral` |
| `segment-index-get-persisted:getHitAsyncJoin` | `56573.614 ops/s` | `56999.180 ops/s` | `+0.75%` | `neutral` |
| `segment-index-get-persisted:getHitSync` | `99751.629 ops/s` | `106588.688 ops/s` | `+6.85%` | `better` |
| `segment-index-get-persisted:getMissAsyncJoin` | `164694.248 ops/s` | `173826.676 ops/s` | `+5.55%` | `better` |
| `segment-index-get-persisted:getMissSync` | `4019268.170 ops/s` | `4001126.754 ops/s` | `-0.45%` | `neutral` |
| `segment-index-hot-partition-put:putHotPartition` | `2983298.781 ops/s` | `3008457.493 ops/s` | `+0.84%` | `neutral` |
| `segment-index-hot-partition-put:putThenGetHotPartition` | `1626220.014 ops/s` | `1735332.086 ops/s` | `+6.71%` | `better` |
| `segment-index-mixed-drain:partitionedIngestMixed` | `464940.491 ops/s` | `433070.097 ops/s` | `-6.85%` | `warning` |
| `segment-index-mixed-drain:partitionedIngestMixed:getWorkload` | `459664.632 ops/s` | `427784.569 ops/s` | `-6.94%` | `warning` |
| `segment-index-mixed-drain:partitionedIngestMixed:putWorkload` | `5275.859 ops/s` | `5285.528 ops/s` | `+0.18%` | `neutral` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed` | `200163.770 ops/s` | `191687.349 ops/s` | `-4.23%` | `warning` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:getWorkload` | `197616.070 ops/s` | `189343.639 ops/s` | `-4.19%` | `warning` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:putWorkload` | `2547.700 ops/s` | `2343.710 ops/s` | `-8.01%` | `worse` |
| `segment-index-persisted-mutation:deleteAsyncJoin` | `2184.247 ops/s` | `2080.818 ops/s` | `-4.74%` | `warning` |
| `segment-index-persisted-mutation:deleteAsyncJoin:diag_directoryCountDelta` | `0.000 ops/s` | `-` | `-` | `removed` |
| `segment-index-persisted-mutation:deleteAsyncJoin:diag_fileBytesDelta` | `55186.967 ops/s` | `-` | `-` | `removed` |
| `segment-index-persisted-mutation:deleteAsyncJoin:diag_fileCountDelta` | `3.999 ops/s` | `-` | `-` | `removed` |
| `segment-index-persisted-mutation:deleteSync` | `2482.403 ops/s` | `2275.828 ops/s` | `-8.32%` | `worse` |
| `segment-index-persisted-mutation:deleteSync:diag_directoryCountDelta` | `0.000 ops/s` | `-` | `-` | `removed` |
| `segment-index-persisted-mutation:deleteSync:diag_fileBytesDelta` | `55701.128 ops/s` | `-` | `-` | `removed` |
| `segment-index-persisted-mutation:deleteSync:diag_fileCountDelta` | `2.983 ops/s` | `-` | `-` | `removed` |
| `segment-index-persisted-mutation:putAsyncJoin` | `2235.485 ops/s` | `1994.815 ops/s` | `-10.77%` | `worse` |
| `segment-index-persisted-mutation:putAsyncJoin:diag_directoryCountDelta` | `0.000 ops/s` | `-` | `-` | `removed` |
| `segment-index-persisted-mutation:putAsyncJoin:diag_fileBytesDelta` | `230018.697 ops/s` | `-` | `-` | `removed` |
| `segment-index-persisted-mutation:putAsyncJoin:diag_fileCountDelta` | `3.666 ops/s` | `-` | `-` | `removed` |
| `segment-index-persisted-mutation:putSync` | `2463.563 ops/s` | `2289.149 ops/s` | `-7.08%` | `worse` |
| `segment-index-persisted-mutation:putSync:diag_directoryCountDelta` | `0.000 ops/s` | `-` | `-` | `removed` |
| `segment-index-persisted-mutation:putSync:diag_fileBytesDelta` | `253980.576 ops/s` | `-` | `-` | `removed` |
| `segment-index-persisted-mutation:putSync:diag_fileCountDelta` | `2.999 ops/s` | `-` | `-` | `removed` |
| `single-chunk-entry-write:writeEntrySteadyState` | `7891969.477 ops/s` | `-` | `-` | `removed` |
| `sorted-data-diff-key-read:readNextKey` | `6608303.151 ops/s` | `-` | `-` | `removed` |
