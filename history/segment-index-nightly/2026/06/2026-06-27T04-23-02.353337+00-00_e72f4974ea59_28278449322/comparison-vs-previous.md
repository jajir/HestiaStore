# Benchmark Comparison

- Candidate labels rerun 3x total and median-merged because the initial regression exceeded threshold with high relative score error: `segment-index-get-multisegment-cold,segment-index-get-multisegment-hot`


- Profile: `segment-index-nightly`
- Baseline SHA: `e72f4974ea597aa48d2725b131d13e39bcfa6872`
- Candidate SHA: `e72f4974ea597aa48d2725b131d13e39bcfa6872`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `segment-index-get-multisegment-cold:getHitAsyncJoin` | `45.186 ops/s` | `41.101 ops/s` | `-9.04%` | `worse` |
| `segment-index-get-multisegment-cold:getHitSync` | `54.836 ops/s` | `42.263 ops/s` | `-22.93%` | `worse` |
| `segment-index-get-multisegment-cold:getMissAsyncJoin` | `184468.702 ops/s` | `186614.783 ops/s` | `+1.16%` | `neutral` |
| `segment-index-get-multisegment-cold:getMissSync` | `3552295.723 ops/s` | `3383906.683 ops/s` | `-4.74%` | `warning` |
| `segment-index-get-multisegment-hot:getHitAsyncJoin` | `89.614 ops/s` | `91.049 ops/s` | `+1.60%` | `neutral` |
| `segment-index-get-multisegment-hot:getHitSync` | `108.645 ops/s` | `89.758 ops/s` | `-17.38%` | `worse` |
| `segment-index-get-multisegment-hot:getMissAsyncJoin` | `185381.987 ops/s` | `185879.417 ops/s` | `+0.27%` | `neutral` |
| `segment-index-get-multisegment-hot:getMissSync` | `3592741.548 ops/s` | `3736066.788 ops/s` | `+3.99%` | `better` |
| `segment-index-get-overlay:getHitAsyncJoin` | `179184.200 ops/s` | `185847.187 ops/s` | `+3.72%` | `better` |
| `segment-index-get-overlay:getHitSync` | `3583424.404 ops/s` | `3689396.334 ops/s` | `+2.96%` | `neutral` |
| `segment-index-get-overlay:getMissAsyncJoin` | `186037.176 ops/s` | `179723.296 ops/s` | `-3.39%` | `warning` |
| `segment-index-get-overlay:getMissSync` | `3872434.326 ops/s` | `3186486.560 ops/s` | `-17.71%` | `worse` |
| `segment-index-get-persisted:getHitAsyncJoin` | `61199.565 ops/s` | `62371.621 ops/s` | `+1.92%` | `neutral` |
| `segment-index-get-persisted:getHitSync` | `113704.431 ops/s` | `115858.945 ops/s` | `+1.89%` | `neutral` |
| `segment-index-get-persisted:getMissAsyncJoin` | `186395.043 ops/s` | `181760.958 ops/s` | `-2.49%` | `neutral` |
| `segment-index-get-persisted:getMissSync` | `3576568.666 ops/s` | `3620176.115 ops/s` | `+1.22%` | `neutral` |
| `segment-index-hot-partition-put:putHotPartition` | `2857074.749 ops/s` | `2829045.454 ops/s` | `-0.98%` | `neutral` |
| `segment-index-hot-partition-put:putThenGetHotPartition` | `1494404.375 ops/s` | `1518465.384 ops/s` | `+1.61%` | `neutral` |
| `segment-index-lifecycle:openAndCheckAndRepairConsistency` | `279.458 ms/op` | `279.302 ms/op` | `-0.06%` | `neutral` |
| `segment-index-lifecycle:openAndCompact` | `302.860 ms/op` | `303.647 ms/op` | `+0.26%` | `neutral` |
| `segment-index-lifecycle:openExisting` | `274.174 ms/op` | `277.574 ms/op` | `+1.24%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed` | `517294.649 ops/s` | `540231.062 ops/s` | `+4.43%` | `better` |
| `segment-index-mixed-drain:partitionedIngestMixed:getWorkload` | `511974.684 ops/s` | `534866.355 ops/s` | `+4.47%` | `better` |
| `segment-index-mixed-drain:partitionedIngestMixed:putWorkload` | `5319.966 ops/s` | `5364.707 ops/s` | `+0.84%` | `neutral` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed` | `257675.023 ops/s` | `267201.644 ops/s` | `+3.70%` | `better` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:getWorkload` | `256127.155 ops/s` | `265743.145 ops/s` | `+3.75%` | `better` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:putWorkload` | `1547.868 ops/s` | `1458.499 ops/s` | `-5.77%` | `warning` |
| `segment-index-persisted-mutation:deleteAsyncJoin` | `2311.433 ops/s` | `2581.718 ops/s` | `+11.69%` | `better` |
| `segment-index-persisted-mutation:deleteSync` | `2646.871 ops/s` | `2760.706 ops/s` | `+4.30%` | `better` |
| `segment-index-persisted-mutation:putAsyncJoin` | `2331.830 ops/s` | `2477.344 ops/s` | `+6.24%` | `better` |
| `segment-index-persisted-mutation:putSync` | `2360.347 ops/s` | `2760.191 ops/s` | `+16.94%` | `better` |
| `single-chunk-entry-write-compact:writeEntrySteadyState` | `8313897.181 ops/s` | `8250598.194 ops/s` | `-0.76%` | `neutral` |
| `single-chunk-entry-write-large:writeEntrySteadyState` | `7751710.391 ops/s` | `7722500.441 ops/s` | `-0.38%` | `neutral` |
| `sorted-data-diff-key-read-compact:readNextKey` | `8573787.320 ops/s` | `7813714.748 ops/s` | `-8.87%` | `worse` |
| `sorted-data-diff-key-read-large:readNextKey` | `6793173.150 ops/s` | `6759888.782 ops/s` | `-0.49%` | `neutral` |
