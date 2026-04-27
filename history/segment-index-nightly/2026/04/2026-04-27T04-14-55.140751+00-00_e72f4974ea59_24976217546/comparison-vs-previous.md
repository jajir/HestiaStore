# Benchmark Comparison

- Profile: `segment-index-nightly`
- Baseline SHA: `e72f4974ea597aa48d2725b131d13e39bcfa6872`
- Candidate SHA: `e72f4974ea597aa48d2725b131d13e39bcfa6872`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `segment-index-get-multisegment-cold:getHitAsyncJoin` | `45.000 ops/s` | `44.293 ops/s` | `-1.57%` | `neutral` |
| `segment-index-get-multisegment-cold:getHitSync` | `40.256 ops/s` | `39.863 ops/s` | `-0.98%` | `neutral` |
| `segment-index-get-multisegment-cold:getMissAsyncJoin` | `170772.784 ops/s` | `257266.129 ops/s` | `+50.65%` | `better` |
| `segment-index-get-multisegment-cold:getMissSync` | `3697206.031 ops/s` | `2692428.064 ops/s` | `-27.18%` | `worse` |
| `segment-index-get-multisegment-hot:getHitAsyncJoin` | `95.564 ops/s` | `102.373 ops/s` | `+7.13%` | `better` |
| `segment-index-get-multisegment-hot:getHitSync` | `87.415 ops/s` | `87.092 ops/s` | `-0.37%` | `neutral` |
| `segment-index-get-multisegment-hot:getMissAsyncJoin` | `171722.483 ops/s` | `257157.160 ops/s` | `+49.75%` | `better` |
| `segment-index-get-multisegment-hot:getMissSync` | `3464283.312 ops/s` | `2692383.337 ops/s` | `-22.28%` | `worse` |
| `segment-index-get-overlay:getHitAsyncJoin` | `162840.256 ops/s` | `251708.580 ops/s` | `+54.57%` | `better` |
| `segment-index-get-overlay:getHitSync` | `4195093.429 ops/s` | `2746525.157 ops/s` | `-34.53%` | `worse` |
| `segment-index-get-overlay:getMissAsyncJoin` | `163960.973 ops/s` | `255605.754 ops/s` | `+55.89%` | `better` |
| `segment-index-get-overlay:getMissSync` | `3904516.584 ops/s` | `2536393.076 ops/s` | `-35.04%` | `worse` |
| `segment-index-get-persisted:getHitAsyncJoin` | `62159.040 ops/s` | `77850.648 ops/s` | `+25.24%` | `better` |
| `segment-index-get-persisted:getHitSync` | `115104.830 ops/s` | `114724.420 ops/s` | `-0.33%` | `neutral` |
| `segment-index-get-persisted:getMissAsyncJoin` | `162369.508 ops/s` | `256772.514 ops/s` | `+58.14%` | `better` |
| `segment-index-get-persisted:getMissSync` | `3797199.585 ops/s` | `2636366.929 ops/s` | `-30.57%` | `worse` |
| `segment-index-hot-partition-put:putHotPartition` | `2708172.237 ops/s` | `1811828.413 ops/s` | `-33.10%` | `worse` |
| `segment-index-hot-partition-put:putThenGetHotPartition` | `1672633.180 ops/s` | `1084697.537 ops/s` | `-35.15%` | `worse` |
| `segment-index-lifecycle:openAndCheckAndRepairConsistency` | `249.334 ms/op` | `134.044 ms/op` | `-46.24%` | `worse` |
| `segment-index-lifecycle:openAndCompact` | `267.048 ms/op` | `154.930 ms/op` | `-41.98%` | `worse` |
| `segment-index-lifecycle:openExisting` | `244.762 ms/op` | `131.867 ms/op` | `-46.12%` | `worse` |
| `segment-index-mixed-drain:partitionedIngestMixed` | `488470.507 ops/s` | `458651.873 ops/s` | `-6.10%` | `warning` |
| `segment-index-mixed-drain:partitionedIngestMixed:getWorkload` | `483095.656 ops/s` | `453254.282 ops/s` | `-6.18%` | `warning` |
| `segment-index-mixed-drain:partitionedIngestMixed:putWorkload` | `5374.851 ops/s` | `5397.591 ops/s` | `+0.42%` | `neutral` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed` | `264854.840 ops/s` | `265928.341 ops/s` | `+0.41%` | `neutral` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:getWorkload` | `263436.392 ops/s` | `264609.262 ops/s` | `+0.45%` | `neutral` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:putWorkload` | `1418.448 ops/s` | `1319.079 ops/s` | `-7.01%` | `worse` |
| `segment-index-persisted-mutation:deleteAsyncJoin` | `1851.569 ops/s` | `1816.593 ops/s` | `-1.89%` | `neutral` |
| `segment-index-persisted-mutation:deleteSync` | `2089.867 ops/s` | `2031.903 ops/s` | `-2.77%` | `neutral` |
| `segment-index-persisted-mutation:putAsyncJoin` | `1902.357 ops/s` | `1869.422 ops/s` | `-1.73%` | `neutral` |
| `segment-index-persisted-mutation:putSync` | `2074.974 ops/s` | `1862.816 ops/s` | `-10.22%` | `worse` |
| `single-chunk-entry-write-compact:writeEntrySteadyState` | `8482266.501 ops/s` | `8943333.893 ops/s` | `+5.44%` | `better` |
| `single-chunk-entry-write-large:writeEntrySteadyState` | `7809560.525 ops/s` | `8030959.646 ops/s` | `+2.83%` | `neutral` |
| `sorted-data-diff-key-read-compact:readNextKey` | `8958433.163 ops/s` | `7984079.222 ops/s` | `-10.88%` | `worse` |
| `sorted-data-diff-key-read-large:readNextKey` | `7230266.541 ops/s` | `7122824.695 ops/s` | `-1.49%` | `neutral` |
