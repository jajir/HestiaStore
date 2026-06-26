# Benchmark Comparison

- Profile: `segment-index-nightly`
- Baseline SHA: `e72f4974ea597aa48d2725b131d13e39bcfa6872`
- Candidate SHA: `e72f4974ea597aa48d2725b131d13e39bcfa6872`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `segment-index-get-multisegment-cold:getHitAsyncJoin` | `44.777 ops/s` | `45.186 ops/s` | `+0.91%` | `neutral` |
| `segment-index-get-multisegment-cold:getHitSync` | `38.127 ops/s` | `54.836 ops/s` | `+43.83%` | `better` |
| `segment-index-get-multisegment-cold:getMissAsyncJoin` | `190270.774 ops/s` | `184468.702 ops/s` | `-3.05%` | `warning` |
| `segment-index-get-multisegment-cold:getMissSync` | `3718700.090 ops/s` | `3552295.723 ops/s` | `-4.47%` | `warning` |
| `segment-index-get-multisegment-hot:getHitAsyncJoin` | `88.611 ops/s` | `89.614 ops/s` | `+1.13%` | `neutral` |
| `segment-index-get-multisegment-hot:getHitSync` | `103.855 ops/s` | `108.645 ops/s` | `+4.61%` | `better` |
| `segment-index-get-multisegment-hot:getMissAsyncJoin` | `187816.085 ops/s` | `185381.987 ops/s` | `-1.30%` | `neutral` |
| `segment-index-get-multisegment-hot:getMissSync` | `3304681.983 ops/s` | `3592741.548 ops/s` | `+8.72%` | `better` |
| `segment-index-get-overlay:getHitAsyncJoin` | `176335.710 ops/s` | `179184.200 ops/s` | `+1.62%` | `neutral` |
| `segment-index-get-overlay:getHitSync` | `3946777.473 ops/s` | `3583424.404 ops/s` | `-9.21%` | `worse` |
| `segment-index-get-overlay:getMissAsyncJoin` | `177935.539 ops/s` | `186037.176 ops/s` | `+4.55%` | `better` |
| `segment-index-get-overlay:getMissSync` | `3859640.236 ops/s` | `3872434.326 ops/s` | `+0.33%` | `neutral` |
| `segment-index-get-persisted:getHitAsyncJoin` | `61051.022 ops/s` | `61199.565 ops/s` | `+0.24%` | `neutral` |
| `segment-index-get-persisted:getHitSync` | `115350.356 ops/s` | `113704.431 ops/s` | `-1.43%` | `neutral` |
| `segment-index-get-persisted:getMissAsyncJoin` | `179257.528 ops/s` | `186395.043 ops/s` | `+3.98%` | `better` |
| `segment-index-get-persisted:getMissSync` | `3726641.616 ops/s` | `3576568.666 ops/s` | `-4.03%` | `warning` |
| `segment-index-hot-partition-put:putHotPartition` | `2802343.561 ops/s` | `2857074.749 ops/s` | `+1.95%` | `neutral` |
| `segment-index-hot-partition-put:putThenGetHotPartition` | `1534127.849 ops/s` | `1494404.375 ops/s` | `-2.59%` | `neutral` |
| `segment-index-lifecycle:openAndCheckAndRepairConsistency` | `245.378 ms/op` | `279.458 ms/op` | `+13.89%` | `better` |
| `segment-index-lifecycle:openAndCompact` | `263.317 ms/op` | `302.860 ms/op` | `+15.02%` | `better` |
| `segment-index-lifecycle:openExisting` | `242.703 ms/op` | `274.174 ms/op` | `+12.97%` | `better` |
| `segment-index-mixed-drain:partitionedIngestMixed` | `515392.072 ops/s` | `517294.649 ops/s` | `+0.37%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed:getWorkload` | `510029.249 ops/s` | `511974.684 ops/s` | `+0.38%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed:putWorkload` | `5362.823 ops/s` | `5319.966 ops/s` | `-0.80%` | `neutral` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed` | `264089.394 ops/s` | `257675.023 ops/s` | `-2.43%` | `neutral` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:getWorkload` | `262711.047 ops/s` | `256127.155 ops/s` | `-2.51%` | `neutral` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:putWorkload` | `1436.991 ops/s` | `1547.868 ops/s` | `+7.72%` | `better` |
| `segment-index-persisted-mutation:deleteAsyncJoin` | `1481.404 ops/s` | `2311.433 ops/s` | `+56.03%` | `better` |
| `segment-index-persisted-mutation:deleteSync` | `1672.426 ops/s` | `2646.871 ops/s` | `+58.27%` | `better` |
| `segment-index-persisted-mutation:putAsyncJoin` | `1449.107 ops/s` | `2331.830 ops/s` | `+60.91%` | `better` |
| `segment-index-persisted-mutation:putSync` | `1555.368 ops/s` | `2360.347 ops/s` | `+51.75%` | `better` |
| `single-chunk-entry-write-compact:writeEntrySteadyState` | `8534012.902 ops/s` | `8313897.181 ops/s` | `-2.58%` | `neutral` |
| `single-chunk-entry-write-large:writeEntrySteadyState` | `7834171.039 ops/s` | `7751710.391 ops/s` | `-1.05%` | `neutral` |
| `sorted-data-diff-key-read-compact:readNextKey` | `9461057.307 ops/s` | `8573787.320 ops/s` | `-9.38%` | `worse` |
| `sorted-data-diff-key-read-large:readNextKey` | `7311348.124 ops/s` | `6793173.150 ops/s` | `-7.09%` | `worse` |
