# Benchmark Comparison

- Profile: `segment-index-nightly`
- Baseline SHA: `e72f4974ea597aa48d2725b131d13e39bcfa6872`
- Candidate SHA: `e72f4974ea597aa48d2725b131d13e39bcfa6872`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `segment-index-get-multisegment-cold:getHitAsyncJoin` | `45.794 ops/s` | `43.587 ops/s` | `-4.82%` | `warning` |
| `segment-index-get-multisegment-cold:getHitSync` | `41.206 ops/s` | `40.791 ops/s` | `-1.01%` | `neutral` |
| `segment-index-get-multisegment-cold:getMissAsyncJoin` | `183902.863 ops/s` | `185305.537 ops/s` | `+0.76%` | `neutral` |
| `segment-index-get-multisegment-cold:getMissSync` | `3942246.091 ops/s` | `3679518.880 ops/s` | `-6.66%` | `warning` |
| `segment-index-get-multisegment-hot:getHitAsyncJoin` | `96.493 ops/s` | `90.038 ops/s` | `-6.69%` | `warning` |
| `segment-index-get-multisegment-hot:getHitSync` | `87.890 ops/s` | `92.959 ops/s` | `+5.77%` | `better` |
| `segment-index-get-multisegment-hot:getMissAsyncJoin` | `185347.611 ops/s` | `183946.194 ops/s` | `-0.76%` | `neutral` |
| `segment-index-get-multisegment-hot:getMissSync` | `3605845.758 ops/s` | `3485215.867 ops/s` | `-3.35%` | `warning` |
| `segment-index-get-overlay:getHitAsyncJoin` | `176728.545 ops/s` | `177598.583 ops/s` | `+0.49%` | `neutral` |
| `segment-index-get-overlay:getHitSync` | `3542036.520 ops/s` | `3633761.583 ops/s` | `+2.59%` | `neutral` |
| `segment-index-get-overlay:getMissAsyncJoin` | `183475.692 ops/s` | `185216.758 ops/s` | `+0.95%` | `neutral` |
| `segment-index-get-overlay:getMissSync` | `3608817.761 ops/s` | `3608305.177 ops/s` | `-0.01%` | `neutral` |
| `segment-index-get-persisted:getHitAsyncJoin` | `63560.602 ops/s` | `63410.335 ops/s` | `-0.24%` | `neutral` |
| `segment-index-get-persisted:getHitSync` | `113378.411 ops/s` | `114803.071 ops/s` | `+1.26%` | `neutral` |
| `segment-index-get-persisted:getMissAsyncJoin` | `184820.373 ops/s` | `185096.193 ops/s` | `+0.15%` | `neutral` |
| `segment-index-get-persisted:getMissSync` | `3325553.926 ops/s` | `3594751.507 ops/s` | `+8.09%` | `better` |
| `segment-index-hot-partition-put:putHotPartition` | `2962569.615 ops/s` | `2743140.217 ops/s` | `-7.41%` | `worse` |
| `segment-index-hot-partition-put:putThenGetHotPartition` | `1632813.433 ops/s` | `1495983.621 ops/s` | `-8.38%` | `worse` |
| `segment-index-lifecycle:openAndCheckAndRepairConsistency` | `277.507 ms/op` | `278.131 ms/op` | `+0.22%` | `neutral` |
| `segment-index-lifecycle:openAndCompact` | `301.481 ms/op` | `300.581 ms/op` | `-0.30%` | `neutral` |
| `segment-index-lifecycle:openExisting` | `278.026 ms/op` | `273.181 ms/op` | `-1.74%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed` | `491157.689 ops/s` | `516768.686 ops/s` | `+5.21%` | `better` |
| `segment-index-mixed-drain:partitionedIngestMixed:getWorkload` | `485802.110 ops/s` | `511407.657 ops/s` | `+5.27%` | `better` |
| `segment-index-mixed-drain:partitionedIngestMixed:putWorkload` | `5355.579 ops/s` | `5361.028 ops/s` | `+0.10%` | `neutral` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed` | `285921.988 ops/s` | `268750.642 ops/s` | `-6.01%` | `warning` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:getWorkload` | `269685.795 ops/s` | `267182.620 ops/s` | `-0.93%` | `neutral` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:putWorkload` | `16236.193 ops/s` | `1568.022 ops/s` | `-90.34%` | `worse` |
| `segment-index-persisted-mutation:deleteAsyncJoin` | `2487.850 ops/s` | `2491.486 ops/s` | `+0.15%` | `neutral` |
| `segment-index-persisted-mutation:deleteSync` | `2740.943 ops/s` | `2733.247 ops/s` | `-0.28%` | `neutral` |
| `segment-index-persisted-mutation:putAsyncJoin` | `2392.344 ops/s` | `2470.626 ops/s` | `+3.27%` | `better` |
| `segment-index-persisted-mutation:putSync` | `2578.417 ops/s` | `2586.777 ops/s` | `+0.32%` | `neutral` |
| `single-chunk-entry-write-compact:writeEntrySteadyState` | `8331263.706 ops/s` | `8174147.843 ops/s` | `-1.89%` | `neutral` |
| `single-chunk-entry-write-large:writeEntrySteadyState` | `7701884.062 ops/s` | `7729726.791 ops/s` | `+0.36%` | `neutral` |
| `sorted-data-diff-key-read-compact:readNextKey` | `8573949.871 ops/s` | `8490792.130 ops/s` | `-0.97%` | `neutral` |
| `sorted-data-diff-key-read-large:readNextKey` | `6698704.192 ops/s` | `6800315.598 ops/s` | `+1.52%` | `neutral` |
