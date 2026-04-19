# Benchmark Comparison

- Candidate labels rerun 3x total and median-merged because the initial regression exceeded threshold with high relative score error: `segment-index-get-multisegment-cold,segment-index-lifecycle`


- Profile: `segment-index-nightly`
- Baseline SHA: `e72f4974ea597aa48d2725b131d13e39bcfa6872`
- Candidate SHA: `e72f4974ea597aa48d2725b131d13e39bcfa6872`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `segment-index-get-multisegment-cold:getHitAsyncJoin` | `45.044 ops/s` | `41.686 ops/s` | `-7.46%` | `worse` |
| `segment-index-get-multisegment-cold:getHitSync` | `38.923 ops/s` | `43.094 ops/s` | `+10.72%` | `better` |
| `segment-index-get-multisegment-cold:getMissAsyncJoin` | `183716.908 ops/s` | `169996.138 ops/s` | `-7.47%` | `worse` |
| `segment-index-get-multisegment-cold:getMissSync` | `3552597.382 ops/s` | `3590383.682 ops/s` | `+1.06%` | `neutral` |
| `segment-index-get-multisegment-hot:getHitAsyncJoin` | `90.478 ops/s` | `95.297 ops/s` | `+5.33%` | `better` |
| `segment-index-get-multisegment-hot:getHitSync` | `89.680 ops/s` | `101.286 ops/s` | `+12.94%` | `better` |
| `segment-index-get-multisegment-hot:getMissAsyncJoin` | `184403.405 ops/s` | `170040.772 ops/s` | `-7.79%` | `worse` |
| `segment-index-get-multisegment-hot:getMissSync` | `3340887.958 ops/s` | `3661219.320 ops/s` | `+9.59%` | `better` |
| `segment-index-get-overlay:getHitAsyncJoin` | `179786.151 ops/s` | `162741.357 ops/s` | `-9.48%` | `worse` |
| `segment-index-get-overlay:getHitSync` | `3828633.975 ops/s` | `3860435.698 ops/s` | `+0.83%` | `neutral` |
| `segment-index-get-overlay:getMissAsyncJoin` | `187238.998 ops/s` | `163296.596 ops/s` | `-12.79%` | `worse` |
| `segment-index-get-overlay:getMissSync` | `3450743.393 ops/s` | `3533129.210 ops/s` | `+2.39%` | `neutral` |
| `segment-index-get-persisted:getHitAsyncJoin` | `66394.954 ops/s` | `63300.796 ops/s` | `-4.66%` | `warning` |
| `segment-index-get-persisted:getHitSync` | `115394.100 ops/s` | `118039.182 ops/s` | `+2.29%` | `neutral` |
| `segment-index-get-persisted:getMissAsyncJoin` | `186533.750 ops/s` | `164478.983 ops/s` | `-11.82%` | `worse` |
| `segment-index-get-persisted:getMissSync` | `3709804.788 ops/s` | `3737327.566 ops/s` | `+0.74%` | `neutral` |
| `segment-index-hot-partition-put:putHotPartition` | `2839565.212 ops/s` | `3021395.060 ops/s` | `+6.40%` | `better` |
| `segment-index-hot-partition-put:putThenGetHotPartition` | `1447484.620 ops/s` | `1655506.970 ops/s` | `+14.37%` | `better` |
| `segment-index-lifecycle:openAndCheckAndRepairConsistency` | `278.203 ms/op` | `247.968 ms/op` | `-10.87%` | `worse` |
| `segment-index-lifecycle:openAndCompact` | `299.625 ms/op` | `268.891 ms/op` | `-10.26%` | `worse` |
| `segment-index-lifecycle:openExisting` | `273.697 ms/op` | `246.062 ms/op` | `-10.10%` | `worse` |
| `segment-index-mixed-drain:partitionedIngestMixed` | `510379.590 ops/s` | `507625.255 ops/s` | `-0.54%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed:getWorkload` | `505029.775 ops/s` | `502299.814 ops/s` | `-0.54%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed:putWorkload` | `5349.815 ops/s` | `5325.442 ops/s` | `-0.46%` | `neutral` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed` | `252684.855 ops/s` | `254412.825 ops/s` | `+0.68%` | `neutral` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:getWorkload` | `251119.866 ops/s` | `252950.967 ops/s` | `+0.73%` | `neutral` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:putWorkload` | `1564.989 ops/s` | `1461.858 ops/s` | `-6.59%` | `warning` |
| `segment-index-persisted-mutation:deleteAsyncJoin` | `2546.723 ops/s` | `2024.829 ops/s` | `-20.49%` | `worse` |
| `segment-index-persisted-mutation:deleteSync` | `2810.190 ops/s` | `2267.660 ops/s` | `-19.31%` | `worse` |
| `segment-index-persisted-mutation:putAsyncJoin` | `2497.246 ops/s` | `2012.892 ops/s` | `-19.40%` | `worse` |
| `segment-index-persisted-mutation:putSync` | `2762.596 ops/s` | `2176.649 ops/s` | `-21.21%` | `worse` |
| `single-chunk-entry-write-compact:writeEntrySteadyState` | `8234395.900 ops/s` | `8549613.464 ops/s` | `+3.83%` | `better` |
| `single-chunk-entry-write-large:writeEntrySteadyState` | `7734087.613 ops/s` | `7855781.820 ops/s` | `+1.57%` | `neutral` |
| `sorted-data-diff-key-read-compact:readNextKey` | `8568840.369 ops/s` | `8689181.654 ops/s` | `+1.40%` | `neutral` |
| `sorted-data-diff-key-read-large:readNextKey` | `6710910.198 ops/s` | `7373826.343 ops/s` | `+9.88%` | `better` |
