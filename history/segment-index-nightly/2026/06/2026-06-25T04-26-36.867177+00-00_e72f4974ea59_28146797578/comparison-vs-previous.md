# Benchmark Comparison

- Candidate labels rerun 3x total and median-merged because the initial regression exceeded threshold with high relative score error: `segment-index-get-multisegment-cold,segment-index-mixed-split-heavy,segment-index-persisted-mutation`


- Profile: `segment-index-nightly`
- Baseline SHA: `e72f4974ea597aa48d2725b131d13e39bcfa6872`
- Candidate SHA: `e72f4974ea597aa48d2725b131d13e39bcfa6872`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `segment-index-get-multisegment-cold:getHitAsyncJoin` | `50.247 ops/s` | `44.777 ops/s` | `-10.89%` | `worse` |
| `segment-index-get-multisegment-cold:getHitSync` | `44.434 ops/s` | `38.127 ops/s` | `-14.19%` | `worse` |
| `segment-index-get-multisegment-cold:getMissAsyncJoin` | `169636.785 ops/s` | `190270.774 ops/s` | `+12.16%` | `better` |
| `segment-index-get-multisegment-cold:getMissSync` | `3796589.179 ops/s` | `3718700.090 ops/s` | `-2.05%` | `neutral` |
| `segment-index-get-multisegment-hot:getHitAsyncJoin` | `89.738 ops/s` | `88.611 ops/s` | `-1.26%` | `neutral` |
| `segment-index-get-multisegment-hot:getHitSync` | `85.705 ops/s` | `103.855 ops/s` | `+21.18%` | `better` |
| `segment-index-get-multisegment-hot:getMissAsyncJoin` | `171049.321 ops/s` | `187816.085 ops/s` | `+9.80%` | `better` |
| `segment-index-get-multisegment-hot:getMissSync` | `3728102.039 ops/s` | `3304681.983 ops/s` | `-11.36%` | `worse` |
| `segment-index-get-overlay:getHitAsyncJoin` | `159600.013 ops/s` | `176335.710 ops/s` | `+10.49%` | `better` |
| `segment-index-get-overlay:getHitSync` | `4186443.326 ops/s` | `3946777.473 ops/s` | `-5.72%` | `warning` |
| `segment-index-get-overlay:getMissAsyncJoin` | `162904.777 ops/s` | `177935.539 ops/s` | `+9.23%` | `better` |
| `segment-index-get-overlay:getMissSync` | `3807759.122 ops/s` | `3859640.236 ops/s` | `+1.36%` | `neutral` |
| `segment-index-get-persisted:getHitAsyncJoin` | `59536.201 ops/s` | `61051.022 ops/s` | `+2.54%` | `neutral` |
| `segment-index-get-persisted:getHitSync` | `111303.888 ops/s` | `115350.356 ops/s` | `+3.64%` | `better` |
| `segment-index-get-persisted:getMissAsyncJoin` | `152446.462 ops/s` | `179257.528 ops/s` | `+17.59%` | `better` |
| `segment-index-get-persisted:getMissSync` | `3793628.601 ops/s` | `3726641.616 ops/s` | `-1.77%` | `neutral` |
| `segment-index-hot-partition-put:putHotPartition` | `2659059.530 ops/s` | `2802343.561 ops/s` | `+5.39%` | `better` |
| `segment-index-hot-partition-put:putThenGetHotPartition` | `1516908.814 ops/s` | `1534127.849 ops/s` | `+1.14%` | `neutral` |
| `segment-index-lifecycle:openAndCheckAndRepairConsistency` | `249.260 ms/op` | `245.378 ms/op` | `-1.56%` | `neutral` |
| `segment-index-lifecycle:openAndCompact` | `269.599 ms/op` | `263.317 ms/op` | `-2.33%` | `neutral` |
| `segment-index-lifecycle:openExisting` | `245.529 ms/op` | `242.703 ms/op` | `-1.15%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed` | `487768.294 ops/s` | `515392.072 ops/s` | `+5.66%` | `better` |
| `segment-index-mixed-drain:partitionedIngestMixed:getWorkload` | `482435.602 ops/s` | `510029.249 ops/s` | `+5.72%` | `better` |
| `segment-index-mixed-drain:partitionedIngestMixed:putWorkload` | `5332.692 ops/s` | `5362.823 ops/s` | `+0.57%` | `neutral` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed` | `300878.375 ops/s` | `264089.394 ops/s` | `-12.23%` | `worse` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:getWorkload` | `263692.800 ops/s` | `262711.047 ops/s` | `-0.37%` | `neutral` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:putWorkload` | `37185.575 ops/s` | `1436.991 ops/s` | `-96.14%` | `worse` |
| `segment-index-persisted-mutation:deleteAsyncJoin` | `2019.164 ops/s` | `1481.404 ops/s` | `-26.63%` | `worse` |
| `segment-index-persisted-mutation:deleteSync` | `2197.176 ops/s` | `1672.426 ops/s` | `-23.88%` | `worse` |
| `segment-index-persisted-mutation:putAsyncJoin` | `2007.446 ops/s` | `1449.107 ops/s` | `-27.81%` | `worse` |
| `segment-index-persisted-mutation:putSync` | `2168.155 ops/s` | `1555.368 ops/s` | `-28.26%` | `worse` |
| `single-chunk-entry-write-compact:writeEntrySteadyState` | `8502717.670 ops/s` | `8534012.902 ops/s` | `+0.37%` | `neutral` |
| `single-chunk-entry-write-large:writeEntrySteadyState` | `7945907.548 ops/s` | `7834171.039 ops/s` | `-1.41%` | `neutral` |
| `sorted-data-diff-key-read-compact:readNextKey` | `8845214.510 ops/s` | `9461057.307 ops/s` | `+6.96%` | `better` |
| `sorted-data-diff-key-read-large:readNextKey` | `7382179.673 ops/s` | `7311348.124 ops/s` | `-0.96%` | `neutral` |
