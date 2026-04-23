# Benchmark Comparison

- Profile: `segment-index-nightly`
- Baseline SHA: `e72f4974ea597aa48d2725b131d13e39bcfa6872`
- Candidate SHA: `e72f4974ea597aa48d2725b131d13e39bcfa6872`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `segment-index-get-multisegment-cold:getHitAsyncJoin` | `41.108 ops/s` | `47.305 ops/s` | `+15.07%` | `better` |
| `segment-index-get-multisegment-cold:getHitSync` | `34.813 ops/s` | `43.527 ops/s` | `+25.03%` | `better` |
| `segment-index-get-multisegment-cold:getMissAsyncJoin` | `183639.437 ops/s` | `183938.643 ops/s` | `+0.16%` | `neutral` |
| `segment-index-get-multisegment-cold:getMissSync` | `3652180.135 ops/s` | `3570965.205 ops/s` | `-2.22%` | `neutral` |
| `segment-index-get-multisegment-hot:getHitAsyncJoin` | `89.578 ops/s` | `102.698 ops/s` | `+14.65%` | `better` |
| `segment-index-get-multisegment-hot:getHitSync` | `89.028 ops/s` | `93.378 ops/s` | `+4.89%` | `better` |
| `segment-index-get-multisegment-hot:getMissAsyncJoin` | `184764.518 ops/s` | `186846.701 ops/s` | `+1.13%` | `neutral` |
| `segment-index-get-multisegment-hot:getMissSync` | `3503615.462 ops/s` | `3522911.034 ops/s` | `+0.55%` | `neutral` |
| `segment-index-get-overlay:getHitAsyncJoin` | `187301.668 ops/s` | `184788.872 ops/s` | `-1.34%` | `neutral` |
| `segment-index-get-overlay:getHitSync` | `3720416.817 ops/s` | `3852776.727 ops/s` | `+3.56%` | `better` |
| `segment-index-get-overlay:getMissAsyncJoin` | `183567.874 ops/s` | `180788.266 ops/s` | `-1.51%` | `neutral` |
| `segment-index-get-overlay:getMissSync` | `3681268.980 ops/s` | `3123952.674 ops/s` | `-15.14%` | `worse` |
| `segment-index-get-persisted:getHitAsyncJoin` | `61557.356 ops/s` | `60523.577 ops/s` | `-1.68%` | `neutral` |
| `segment-index-get-persisted:getHitSync` | `116213.083 ops/s` | `114563.759 ops/s` | `-1.42%` | `neutral` |
| `segment-index-get-persisted:getMissAsyncJoin` | `179074.071 ops/s` | `183435.052 ops/s` | `+2.44%` | `neutral` |
| `segment-index-get-persisted:getMissSync` | `3921952.015 ops/s` | `3623228.323 ops/s` | `-7.62%` | `worse` |
| `segment-index-hot-partition-put:putHotPartition` | `2412956.578 ops/s` | `2719345.941 ops/s` | `+12.70%` | `better` |
| `segment-index-hot-partition-put:putThenGetHotPartition` | `1593379.698 ops/s` | `1422625.830 ops/s` | `-10.72%` | `worse` |
| `segment-index-lifecycle:openAndCheckAndRepairConsistency` | `277.820 ms/op` | `295.742 ms/op` | `+6.45%` | `better` |
| `segment-index-lifecycle:openAndCompact` | `301.582 ms/op` | `301.479 ms/op` | `-0.03%` | `neutral` |
| `segment-index-lifecycle:openExisting` | `274.155 ms/op` | `280.785 ms/op` | `+2.42%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed` | `492877.968 ops/s` | `504998.851 ops/s` | `+2.46%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed:getWorkload` | `487520.091 ops/s` | `499626.608 ops/s` | `+2.48%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed:putWorkload` | `5357.877 ops/s` | `5372.243 ops/s` | `+0.27%` | `neutral` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed` | `263327.840 ops/s` | `-` | `-` | `removed` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:getWorkload` | `261880.357 ops/s` | `-` | `-` | `removed` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:putWorkload` | `1447.483 ops/s` | `-` | `-` | `removed` |
| `segment-index-persisted-mutation:deleteAsyncJoin` | `2494.425 ops/s` | `2517.013 ops/s` | `+0.91%` | `neutral` |
| `segment-index-persisted-mutation:deleteSync` | `2619.607 ops/s` | `2734.558 ops/s` | `+4.39%` | `better` |
| `segment-index-persisted-mutation:putAsyncJoin` | `2393.316 ops/s` | `2513.416 ops/s` | `+5.02%` | `better` |
| `segment-index-persisted-mutation:putSync` | `2581.500 ops/s` | `2693.377 ops/s` | `+4.33%` | `better` |
| `single-chunk-entry-write-compact:writeEntrySteadyState` | `8337781.085 ops/s` | `8279542.714 ops/s` | `-0.70%` | `neutral` |
| `single-chunk-entry-write-large:writeEntrySteadyState` | `7845520.562 ops/s` | `7807546.837 ops/s` | `-0.48%` | `neutral` |
| `sorted-data-diff-key-read-compact:readNextKey` | `8551319.074 ops/s` | `8206728.320 ops/s` | `-4.03%` | `warning` |
| `sorted-data-diff-key-read-large:readNextKey` | `6669595.393 ops/s` | `6660607.642 ops/s` | `-0.13%` | `neutral` |
