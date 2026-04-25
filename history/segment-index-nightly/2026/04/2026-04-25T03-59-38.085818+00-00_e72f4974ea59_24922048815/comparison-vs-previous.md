# Benchmark Comparison

- Candidate labels rerun 3x total and median-merged because the initial regression exceeded threshold with high relative score error: `segment-index-mixed-split-heavy`


- Profile: `segment-index-nightly`
- Baseline SHA: `e72f4974ea597aa48d2725b131d13e39bcfa6872`
- Candidate SHA: `e72f4974ea597aa48d2725b131d13e39bcfa6872`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `segment-index-get-multisegment-cold:getHitAsyncJoin` | `40.628 ops/s` | `55.033 ops/s` | `+35.46%` | `better` |
| `segment-index-get-multisegment-cold:getHitSync` | `41.967 ops/s` | `41.367 ops/s` | `-1.43%` | `neutral` |
| `segment-index-get-multisegment-cold:getMissAsyncJoin` | `237396.332 ops/s` | `172359.918 ops/s` | `-27.40%` | `worse` |
| `segment-index-get-multisegment-cold:getMissSync` | `5059055.894 ops/s` | `3795780.468 ops/s` | `-24.97%` | `worse` |
| `segment-index-get-multisegment-hot:getHitAsyncJoin` | `90.098 ops/s` | `101.867 ops/s` | `+13.06%` | `better` |
| `segment-index-get-multisegment-hot:getHitSync` | `79.926 ops/s` | `78.349 ops/s` | `-1.97%` | `neutral` |
| `segment-index-get-multisegment-hot:getMissAsyncJoin` | `239645.279 ops/s` | `173200.603 ops/s` | `-27.73%` | `worse` |
| `segment-index-get-multisegment-hot:getMissSync` | `5016677.151 ops/s` | `3760760.593 ops/s` | `-25.03%` | `worse` |
| `segment-index-get-overlay:getHitAsyncJoin` | `232647.898 ops/s` | `163213.794 ops/s` | `-29.85%` | `worse` |
| `segment-index-get-overlay:getHitSync` | `4581589.548 ops/s` | `3712722.111 ops/s` | `-18.96%` | `worse` |
| `segment-index-get-overlay:getMissAsyncJoin` | `240187.888 ops/s` | `167017.325 ops/s` | `-30.46%` | `worse` |
| `segment-index-get-overlay:getMissSync` | `4695337.807 ops/s` | `3830704.257 ops/s` | `-18.41%` | `worse` |
| `segment-index-get-persisted:getHitAsyncJoin` | `73880.716 ops/s` | `63899.876 ops/s` | `-13.51%` | `worse` |
| `segment-index-get-persisted:getHitSync` | `159055.686 ops/s` | `117152.614 ops/s` | `-26.34%` | `worse` |
| `segment-index-get-persisted:getMissAsyncJoin` | `240957.906 ops/s` | `167633.492 ops/s` | `-30.43%` | `worse` |
| `segment-index-get-persisted:getMissSync` | `4578793.502 ops/s` | `3978329.912 ops/s` | `-13.11%` | `worse` |
| `segment-index-hot-partition-put:putHotPartition` | `3955162.399 ops/s` | `3040506.518 ops/s` | `-23.13%` | `worse` |
| `segment-index-hot-partition-put:putThenGetHotPartition` | `2144556.632 ops/s` | `1673881.611 ops/s` | `-21.95%` | `worse` |
| `segment-index-lifecycle:openAndCheckAndRepairConsistency` | `216.577 ms/op` | `243.982 ms/op` | `+12.65%` | `better` |
| `segment-index-lifecycle:openAndCompact` | `232.944 ms/op` | `263.664 ms/op` | `+13.19%` | `better` |
| `segment-index-lifecycle:openExisting` | `215.473 ms/op` | `240.013 ms/op` | `+11.39%` | `better` |
| `segment-index-mixed-drain:partitionedIngestMixed` | `779724.495 ops/s` | `524675.011 ops/s` | `-32.71%` | `worse` |
| `segment-index-mixed-drain:partitionedIngestMixed:getWorkload` | `774378.562 ops/s` | `519361.718 ops/s` | `-32.93%` | `worse` |
| `segment-index-mixed-drain:partitionedIngestMixed:putWorkload` | `5345.934 ops/s` | `5313.293 ops/s` | `-0.61%` | `neutral` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed` | `415812.259 ops/s` | `268605.036 ops/s` | `-35.40%` | `worse` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:getWorkload` | `362211.585 ops/s` | `267147.320 ops/s` | `-26.25%` | `worse` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:putWorkload` | `53600.675 ops/s` | `1457.930 ops/s` | `-97.28%` | `worse` |
| `segment-index-persisted-mutation:deleteAsyncJoin` | `747.610 ops/s` | `1981.842 ops/s` | `+165.09%` | `better` |
| `segment-index-persisted-mutation:deleteSync` | `511.916 ops/s` | `2160.470 ops/s` | `+322.04%` | `better` |
| `segment-index-persisted-mutation:putAsyncJoin` | `970.985 ops/s` | `1896.215 ops/s` | `+95.29%` | `better` |
| `segment-index-persisted-mutation:putSync` | `790.179 ops/s` | `2055.063 ops/s` | `+160.08%` | `better` |
| `single-chunk-entry-write-compact:writeEntrySteadyState` | `11028472.941 ops/s` | `8437894.646 ops/s` | `-23.49%` | `worse` |
| `single-chunk-entry-write-large:writeEntrySteadyState` | `10196209.302 ops/s` | `7919892.676 ops/s` | `-22.33%` | `worse` |
| `sorted-data-diff-key-read-compact:readNextKey` | `10957876.558 ops/s` | `9105654.211 ops/s` | `-16.90%` | `worse` |
| `sorted-data-diff-key-read-large:readNextKey` | `8580481.806 ops/s` | `7514772.215 ops/s` | `-12.42%` | `worse` |
