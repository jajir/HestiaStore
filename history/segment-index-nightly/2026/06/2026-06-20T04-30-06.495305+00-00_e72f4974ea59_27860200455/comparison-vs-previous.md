# Benchmark Comparison

- Candidate labels rerun 3x total and median-merged because the initial regression exceeded threshold with high relative score error: `segment-index-get-multisegment-cold,segment-index-get-multisegment-hot`


- Profile: `segment-index-nightly`
- Baseline SHA: `e72f4974ea597aa48d2725b131d13e39bcfa6872`
- Candidate SHA: `e72f4974ea597aa48d2725b131d13e39bcfa6872`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `segment-index-get-multisegment-cold:getHitAsyncJoin` | `58.143 ops/s` | `45.794 ops/s` | `-21.24%` | `worse` |
| `segment-index-get-multisegment-cold:getHitSync` | `42.522 ops/s` | `41.206 ops/s` | `-3.09%` | `warning` |
| `segment-index-get-multisegment-cold:getMissAsyncJoin` | `168820.490 ops/s` | `183902.863 ops/s` | `+8.93%` | `better` |
| `segment-index-get-multisegment-cold:getMissSync` | `3448713.765 ops/s` | `3942246.091 ops/s` | `+14.31%` | `better` |
| `segment-index-get-multisegment-hot:getHitAsyncJoin` | `100.889 ops/s` | `96.493 ops/s` | `-4.36%` | `warning` |
| `segment-index-get-multisegment-hot:getHitSync` | `91.643 ops/s` | `87.890 ops/s` | `-4.10%` | `warning` |
| `segment-index-get-multisegment-hot:getMissAsyncJoin` | `167671.969 ops/s` | `185347.611 ops/s` | `+10.54%` | `better` |
| `segment-index-get-multisegment-hot:getMissSync` | `3553518.088 ops/s` | `3605845.758 ops/s` | `+1.47%` | `neutral` |
| `segment-index-get-overlay:getHitAsyncJoin` | `160264.953 ops/s` | `176728.545 ops/s` | `+10.27%` | `better` |
| `segment-index-get-overlay:getHitSync` | `3815169.024 ops/s` | `3542036.520 ops/s` | `-7.16%` | `worse` |
| `segment-index-get-overlay:getMissAsyncJoin` | `153510.613 ops/s` | `183475.692 ops/s` | `+19.52%` | `better` |
| `segment-index-get-overlay:getMissSync` | `3993313.003 ops/s` | `3608817.761 ops/s` | `-9.63%` | `worse` |
| `segment-index-get-persisted:getHitAsyncJoin` | `58180.563 ops/s` | `63560.602 ops/s` | `+9.25%` | `better` |
| `segment-index-get-persisted:getHitSync` | `112759.918 ops/s` | `113378.411 ops/s` | `+0.55%` | `neutral` |
| `segment-index-get-persisted:getMissAsyncJoin` | `154262.166 ops/s` | `184820.373 ops/s` | `+19.81%` | `better` |
| `segment-index-get-persisted:getMissSync` | `3339067.632 ops/s` | `3325553.926 ops/s` | `-0.40%` | `neutral` |
| `segment-index-hot-partition-put:putHotPartition` | `2982169.444 ops/s` | `2962569.615 ops/s` | `-0.66%` | `neutral` |
| `segment-index-hot-partition-put:putThenGetHotPartition` | `1552105.380 ops/s` | `1632813.433 ops/s` | `+5.20%` | `better` |
| `segment-index-lifecycle:openAndCheckAndRepairConsistency` | `250.116 ms/op` | `277.507 ms/op` | `+10.95%` | `better` |
| `segment-index-lifecycle:openAndCompact` | `269.981 ms/op` | `301.481 ms/op` | `+11.67%` | `better` |
| `segment-index-lifecycle:openExisting` | `248.998 ms/op` | `278.026 ms/op` | `+11.66%` | `better` |
| `segment-index-mixed-drain:partitionedIngestMixed` | `517152.591 ops/s` | `491157.689 ops/s` | `-5.03%` | `warning` |
| `segment-index-mixed-drain:partitionedIngestMixed:getWorkload` | `511805.626 ops/s` | `485802.110 ops/s` | `-5.08%` | `warning` |
| `segment-index-mixed-drain:partitionedIngestMixed:putWorkload` | `5346.965 ops/s` | `5355.579 ops/s` | `+0.16%` | `neutral` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed` | `258415.663 ops/s` | `285921.988 ops/s` | `+10.64%` | `better` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:getWorkload` | `257012.946 ops/s` | `269685.795 ops/s` | `+4.93%` | `better` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:putWorkload` | `1443.146 ops/s` | `16236.193 ops/s` | `+1025.06%` | `better` |
| `segment-index-persisted-mutation:deleteAsyncJoin` | `2041.702 ops/s` | `2487.850 ops/s` | `+21.85%` | `better` |
| `segment-index-persisted-mutation:deleteSync` | `2269.176 ops/s` | `2740.943 ops/s` | `+20.79%` | `better` |
| `segment-index-persisted-mutation:putAsyncJoin` | `2011.274 ops/s` | `2392.344 ops/s` | `+18.95%` | `better` |
| `segment-index-persisted-mutation:putSync` | `2197.006 ops/s` | `2578.417 ops/s` | `+17.36%` | `better` |
| `single-chunk-entry-write-compact:writeEntrySteadyState` | `8301418.955 ops/s` | `8331263.706 ops/s` | `+0.36%` | `neutral` |
| `single-chunk-entry-write-large:writeEntrySteadyState` | `7885065.550 ops/s` | `7701884.062 ops/s` | `-2.32%` | `neutral` |
| `sorted-data-diff-key-read-compact:readNextKey` | `8921693.355 ops/s` | `8573949.871 ops/s` | `-3.90%` | `warning` |
| `sorted-data-diff-key-read-large:readNextKey` | `7220162.076 ops/s` | `6698704.192 ops/s` | `-7.22%` | `worse` |
