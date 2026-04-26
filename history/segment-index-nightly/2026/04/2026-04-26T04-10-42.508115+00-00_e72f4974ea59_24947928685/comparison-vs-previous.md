# Benchmark Comparison

- Candidate labels rerun 3x total and median-merged because the initial regression exceeded threshold with high relative score error: `segment-index-get-multisegment-cold`


- Profile: `segment-index-nightly`
- Baseline SHA: `e72f4974ea597aa48d2725b131d13e39bcfa6872`
- Candidate SHA: `e72f4974ea597aa48d2725b131d13e39bcfa6872`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `segment-index-get-multisegment-cold:getHitAsyncJoin` | `55.033 ops/s` | `45.000 ops/s` | `-18.23%` | `worse` |
| `segment-index-get-multisegment-cold:getHitSync` | `41.367 ops/s` | `40.256 ops/s` | `-2.69%` | `neutral` |
| `segment-index-get-multisegment-cold:getMissAsyncJoin` | `172359.918 ops/s` | `170772.784 ops/s` | `-0.92%` | `neutral` |
| `segment-index-get-multisegment-cold:getMissSync` | `3795780.468 ops/s` | `3697206.031 ops/s` | `-2.60%` | `neutral` |
| `segment-index-get-multisegment-hot:getHitAsyncJoin` | `101.867 ops/s` | `95.564 ops/s` | `-6.19%` | `warning` |
| `segment-index-get-multisegment-hot:getHitSync` | `78.349 ops/s` | `87.415 ops/s` | `+11.57%` | `better` |
| `segment-index-get-multisegment-hot:getMissAsyncJoin` | `173200.603 ops/s` | `171722.483 ops/s` | `-0.85%` | `neutral` |
| `segment-index-get-multisegment-hot:getMissSync` | `3760760.593 ops/s` | `3464283.312 ops/s` | `-7.88%` | `worse` |
| `segment-index-get-overlay:getHitAsyncJoin` | `163213.794 ops/s` | `162840.256 ops/s` | `-0.23%` | `neutral` |
| `segment-index-get-overlay:getHitSync` | `3712722.111 ops/s` | `4195093.429 ops/s` | `+12.99%` | `better` |
| `segment-index-get-overlay:getMissAsyncJoin` | `167017.325 ops/s` | `163960.973 ops/s` | `-1.83%` | `neutral` |
| `segment-index-get-overlay:getMissSync` | `3830704.257 ops/s` | `3904516.584 ops/s` | `+1.93%` | `neutral` |
| `segment-index-get-persisted:getHitAsyncJoin` | `63899.876 ops/s` | `62159.040 ops/s` | `-2.72%` | `neutral` |
| `segment-index-get-persisted:getHitSync` | `117152.614 ops/s` | `115104.830 ops/s` | `-1.75%` | `neutral` |
| `segment-index-get-persisted:getMissAsyncJoin` | `167633.492 ops/s` | `162369.508 ops/s` | `-3.14%` | `warning` |
| `segment-index-get-persisted:getMissSync` | `3978329.912 ops/s` | `3797199.585 ops/s` | `-4.55%` | `warning` |
| `segment-index-hot-partition-put:putHotPartition` | `3040506.518 ops/s` | `2708172.237 ops/s` | `-10.93%` | `worse` |
| `segment-index-hot-partition-put:putThenGetHotPartition` | `1673881.611 ops/s` | `1672633.180 ops/s` | `-0.07%` | `neutral` |
| `segment-index-lifecycle:openAndCheckAndRepairConsistency` | `243.982 ms/op` | `249.334 ms/op` | `+2.19%` | `neutral` |
| `segment-index-lifecycle:openAndCompact` | `263.664 ms/op` | `267.048 ms/op` | `+1.28%` | `neutral` |
| `segment-index-lifecycle:openExisting` | `240.013 ms/op` | `244.762 ms/op` | `+1.98%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed` | `524675.011 ops/s` | `488470.507 ops/s` | `-6.90%` | `warning` |
| `segment-index-mixed-drain:partitionedIngestMixed:getWorkload` | `519361.718 ops/s` | `483095.656 ops/s` | `-6.98%` | `warning` |
| `segment-index-mixed-drain:partitionedIngestMixed:putWorkload` | `5313.293 ops/s` | `5374.851 ops/s` | `+1.16%` | `neutral` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed` | `268605.036 ops/s` | `264854.840 ops/s` | `-1.40%` | `neutral` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:getWorkload` | `267147.320 ops/s` | `263436.392 ops/s` | `-1.39%` | `neutral` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:putWorkload` | `1457.930 ops/s` | `1418.448 ops/s` | `-2.71%` | `neutral` |
| `segment-index-persisted-mutation:deleteAsyncJoin` | `1981.842 ops/s` | `1851.569 ops/s` | `-6.57%` | `warning` |
| `segment-index-persisted-mutation:deleteSync` | `2160.470 ops/s` | `2089.867 ops/s` | `-3.27%` | `warning` |
| `segment-index-persisted-mutation:putAsyncJoin` | `1896.215 ops/s` | `1902.357 ops/s` | `+0.32%` | `neutral` |
| `segment-index-persisted-mutation:putSync` | `2055.063 ops/s` | `2074.974 ops/s` | `+0.97%` | `neutral` |
| `single-chunk-entry-write-compact:writeEntrySteadyState` | `8437894.646 ops/s` | `8482266.501 ops/s` | `+0.53%` | `neutral` |
| `single-chunk-entry-write-large:writeEntrySteadyState` | `7919892.676 ops/s` | `7809560.525 ops/s` | `-1.39%` | `neutral` |
| `sorted-data-diff-key-read-compact:readNextKey` | `9105654.211 ops/s` | `8958433.163 ops/s` | `-1.62%` | `neutral` |
| `sorted-data-diff-key-read-large:readNextKey` | `7514772.215 ops/s` | `7230266.541 ops/s` | `-3.79%` | `warning` |
