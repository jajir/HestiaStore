# Benchmark Comparison

- Profile: `segment-index-nightly`
- Baseline SHA: `e72f4974ea597aa48d2725b131d13e39bcfa6872`
- Candidate SHA: `e72f4974ea597aa48d2725b131d13e39bcfa6872`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `segment-index-get-multisegment-cold:getHitAsyncJoin` | `43.587 ops/s` | `48.081 ops/s` | `+10.31%` | `better` |
| `segment-index-get-multisegment-cold:getHitSync` | `40.791 ops/s` | `52.086 ops/s` | `+27.69%` | `better` |
| `segment-index-get-multisegment-cold:getMissAsyncJoin` | `185305.537 ops/s` | `187003.526 ops/s` | `+0.92%` | `neutral` |
| `segment-index-get-multisegment-cold:getMissSync` | `3679518.880 ops/s` | `3753296.366 ops/s` | `+2.01%` | `neutral` |
| `segment-index-get-multisegment-hot:getHitAsyncJoin` | `90.038 ops/s` | `87.346 ops/s` | `-2.99%` | `neutral` |
| `segment-index-get-multisegment-hot:getHitSync` | `92.959 ops/s` | `88.785 ops/s` | `-4.49%` | `warning` |
| `segment-index-get-multisegment-hot:getMissAsyncJoin` | `183946.194 ops/s` | `185469.965 ops/s` | `+0.83%` | `neutral` |
| `segment-index-get-multisegment-hot:getMissSync` | `3485215.867 ops/s` | `3628332.702 ops/s` | `+4.11%` | `better` |
| `segment-index-get-overlay:getHitAsyncJoin` | `177598.583 ops/s` | `177982.480 ops/s` | `+0.22%` | `neutral` |
| `segment-index-get-overlay:getHitSync` | `3633761.583 ops/s` | `3656918.412 ops/s` | `+0.64%` | `neutral` |
| `segment-index-get-overlay:getMissAsyncJoin` | `185216.758 ops/s` | `179942.522 ops/s` | `-2.85%` | `neutral` |
| `segment-index-get-overlay:getMissSync` | `3608305.177 ops/s` | `3690449.583 ops/s` | `+2.28%` | `neutral` |
| `segment-index-get-persisted:getHitAsyncJoin` | `63410.335 ops/s` | `61081.870 ops/s` | `-3.67%` | `warning` |
| `segment-index-get-persisted:getHitSync` | `114803.071 ops/s` | `113691.484 ops/s` | `-0.97%` | `neutral` |
| `segment-index-get-persisted:getMissAsyncJoin` | `185096.193 ops/s` | `179191.292 ops/s` | `-3.19%` | `warning` |
| `segment-index-get-persisted:getMissSync` | `3594751.507 ops/s` | `3469891.285 ops/s` | `-3.47%` | `warning` |
| `segment-index-hot-partition-put:putHotPartition` | `2743140.217 ops/s` | `2847295.081 ops/s` | `+3.80%` | `better` |
| `segment-index-hot-partition-put:putThenGetHotPartition` | `1495983.621 ops/s` | `1400574.519 ops/s` | `-6.38%` | `warning` |
| `segment-index-lifecycle:openAndCheckAndRepairConsistency` | `278.131 ms/op` | `277.887 ms/op` | `-0.09%` | `neutral` |
| `segment-index-lifecycle:openAndCompact` | `300.581 ms/op` | `302.250 ms/op` | `+0.56%` | `neutral` |
| `segment-index-lifecycle:openExisting` | `273.181 ms/op` | `275.829 ms/op` | `+0.97%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed` | `516768.686 ops/s` | `519676.299 ops/s` | `+0.56%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed:getWorkload` | `511407.657 ops/s` | `514326.855 ops/s` | `+0.57%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed:putWorkload` | `5361.028 ops/s` | `5349.445 ops/s` | `-0.22%` | `neutral` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed` | `268750.642 ops/s` | `259604.522 ops/s` | `-3.40%` | `warning` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:getWorkload` | `267182.620 ops/s` | `258042.366 ops/s` | `-3.42%` | `warning` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:putWorkload` | `1568.022 ops/s` | `1562.155 ops/s` | `-0.37%` | `neutral` |
| `segment-index-persisted-mutation:deleteAsyncJoin` | `2491.486 ops/s` | `2563.214 ops/s` | `+2.88%` | `neutral` |
| `segment-index-persisted-mutation:deleteSync` | `2733.247 ops/s` | `2748.543 ops/s` | `+0.56%` | `neutral` |
| `segment-index-persisted-mutation:putAsyncJoin` | `2470.626 ops/s` | `2492.533 ops/s` | `+0.89%` | `neutral` |
| `segment-index-persisted-mutation:putSync` | `2586.777 ops/s` | `2751.180 ops/s` | `+6.36%` | `better` |
| `single-chunk-entry-write-compact:writeEntrySteadyState` | `8174147.843 ops/s` | `8227522.645 ops/s` | `+0.65%` | `neutral` |
| `single-chunk-entry-write-large:writeEntrySteadyState` | `7729726.791 ops/s` | `7753680.497 ops/s` | `+0.31%` | `neutral` |
| `sorted-data-diff-key-read-compact:readNextKey` | `8490792.130 ops/s` | `8471452.008 ops/s` | `-0.23%` | `neutral` |
| `sorted-data-diff-key-read-large:readNextKey` | `6800315.598 ops/s` | `6389431.842 ops/s` | `-6.04%` | `warning` |
