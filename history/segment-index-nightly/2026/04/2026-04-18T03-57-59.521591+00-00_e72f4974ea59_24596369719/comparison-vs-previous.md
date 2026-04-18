# Benchmark Comparison

- Candidate labels rerun 3x total and median-merged because the initial regression exceeded threshold with high relative score error: `segment-index-get-multisegment-cold`


- Profile: `segment-index-nightly`
- Baseline SHA: `e72f4974ea597aa48d2725b131d13e39bcfa6872`
- Candidate SHA: `e72f4974ea597aa48d2725b131d13e39bcfa6872`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `segment-index-get-multisegment-cold:getHitAsyncJoin` | `46.207 ops/s` | `45.044 ops/s` | `-2.52%` | `neutral` |
| `segment-index-get-multisegment-cold:getHitSync` | `49.033 ops/s` | `38.923 ops/s` | `-20.62%` | `worse` |
| `segment-index-get-multisegment-cold:getMissAsyncJoin` | `173028.265 ops/s` | `183716.908 ops/s` | `+6.18%` | `better` |
| `segment-index-get-multisegment-cold:getMissSync` | `3719401.049 ops/s` | `3552597.382 ops/s` | `-4.48%` | `warning` |
| `segment-index-get-multisegment-hot:getHitAsyncJoin` | `96.037 ops/s` | `90.478 ops/s` | `-5.79%` | `warning` |
| `segment-index-get-multisegment-hot:getHitSync` | `93.975 ops/s` | `89.680 ops/s` | `-4.57%` | `warning` |
| `segment-index-get-multisegment-hot:getMissAsyncJoin` | `171499.878 ops/s` | `184403.405 ops/s` | `+7.52%` | `better` |
| `segment-index-get-multisegment-hot:getMissSync` | `3795984.129 ops/s` | `3340887.958 ops/s` | `-11.99%` | `worse` |
| `segment-index-get-overlay:getHitAsyncJoin` | `165161.459 ops/s` | `179786.151 ops/s` | `+8.85%` | `better` |
| `segment-index-get-overlay:getHitSync` | `3746814.891 ops/s` | `3828633.975 ops/s` | `+2.18%` | `neutral` |
| `segment-index-get-overlay:getMissAsyncJoin` | `167178.953 ops/s` | `187238.998 ops/s` | `+12.00%` | `better` |
| `segment-index-get-overlay:getMissSync` | `3690014.655 ops/s` | `3450743.393 ops/s` | `-6.48%` | `warning` |
| `segment-index-get-persisted:getHitAsyncJoin` | `61800.741 ops/s` | `66394.954 ops/s` | `+7.43%` | `better` |
| `segment-index-get-persisted:getHitSync` | `112452.479 ops/s` | `115394.100 ops/s` | `+2.62%` | `neutral` |
| `segment-index-get-persisted:getMissAsyncJoin` | `166593.759 ops/s` | `186533.750 ops/s` | `+11.97%` | `better` |
| `segment-index-get-persisted:getMissSync` | `3660380.911 ops/s` | `3709804.788 ops/s` | `+1.35%` | `neutral` |
| `segment-index-hot-partition-put:putHotPartition` | `3141585.590 ops/s` | `2839565.212 ops/s` | `-9.61%` | `worse` |
| `segment-index-hot-partition-put:putThenGetHotPartition` | `1606593.481 ops/s` | `1447484.620 ops/s` | `-9.90%` | `worse` |
| `segment-index-lifecycle:openAndCheckAndRepairConsistency` | `242.731 ms/op` | `278.203 ms/op` | `+14.61%` | `better` |
| `segment-index-lifecycle:openAndCompact` | `264.175 ms/op` | `299.625 ms/op` | `+13.42%` | `better` |
| `segment-index-lifecycle:openExisting` | `238.847 ms/op` | `273.697 ms/op` | `+14.59%` | `better` |
| `segment-index-mixed-drain:partitionedIngestMixed` | `554256.099 ops/s` | `510379.590 ops/s` | `-7.92%` | `worse` |
| `segment-index-mixed-drain:partitionedIngestMixed:getWorkload` | `548934.597 ops/s` | `505029.775 ops/s` | `-8.00%` | `worse` |
| `segment-index-mixed-drain:partitionedIngestMixed:putWorkload` | `5321.501 ops/s` | `5349.815 ops/s` | `+0.53%` | `neutral` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed` | `263357.012 ops/s` | `252684.855 ops/s` | `-4.05%` | `warning` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:getWorkload` | `261770.289 ops/s` | `251119.866 ops/s` | `-4.07%` | `warning` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:putWorkload` | `1568.194 ops/s` | `1564.989 ops/s` | `-0.20%` | `neutral` |
| `segment-index-persisted-mutation:deleteAsyncJoin` | `2011.990 ops/s` | `2546.723 ops/s` | `+26.58%` | `better` |
| `segment-index-persisted-mutation:deleteSync` | `2260.871 ops/s` | `2810.190 ops/s` | `+24.30%` | `better` |
| `segment-index-persisted-mutation:putAsyncJoin` | `2001.871 ops/s` | `2497.246 ops/s` | `+24.75%` | `better` |
| `segment-index-persisted-mutation:putSync` | `2186.783 ops/s` | `2762.596 ops/s` | `+26.33%` | `better` |
| `single-chunk-entry-write-compact:writeEntrySteadyState` | `8237689.207 ops/s` | `8234395.900 ops/s` | `-0.04%` | `neutral` |
| `single-chunk-entry-write-large:writeEntrySteadyState` | `7857977.032 ops/s` | `7734087.613 ops/s` | `-1.58%` | `neutral` |
| `sorted-data-diff-key-read-compact:readNextKey` | `8637481.513 ops/s` | `8568840.369 ops/s` | `-0.79%` | `neutral` |
| `sorted-data-diff-key-read-large:readNextKey` | `7508381.256 ops/s` | `6710910.198 ops/s` | `-10.62%` | `worse` |
