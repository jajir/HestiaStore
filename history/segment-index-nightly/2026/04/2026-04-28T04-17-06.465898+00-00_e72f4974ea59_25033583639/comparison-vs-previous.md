# Benchmark Comparison

- Candidate labels rerun 3x total and median-merged because the initial regression exceeded threshold with high relative score error: `segment-index-get-multisegment-hot,segment-index-get-persisted`


- Profile: `segment-index-nightly`
- Baseline SHA: `e72f4974ea597aa48d2725b131d13e39bcfa6872`
- Candidate SHA: `e72f4974ea597aa48d2725b131d13e39bcfa6872`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `segment-index-get-multisegment-cold:getHitAsyncJoin` | `44.293 ops/s` | `47.298 ops/s` | `+6.78%` | `better` |
| `segment-index-get-multisegment-cold:getHitSync` | `39.863 ops/s` | `44.734 ops/s` | `+12.22%` | `better` |
| `segment-index-get-multisegment-cold:getMissAsyncJoin` | `257266.129 ops/s` | `187147.428 ops/s` | `-27.26%` | `worse` |
| `segment-index-get-multisegment-cold:getMissSync` | `2692428.064 ops/s` | `3359822.060 ops/s` | `+24.79%` | `better` |
| `segment-index-get-multisegment-hot:getHitAsyncJoin` | `102.373 ops/s` | `92.960 ops/s` | `-9.19%` | `worse` |
| `segment-index-get-multisegment-hot:getHitSync` | `87.092 ops/s` | `89.072 ops/s` | `+2.27%` | `neutral` |
| `segment-index-get-multisegment-hot:getMissAsyncJoin` | `257157.160 ops/s` | `186466.856 ops/s` | `-27.49%` | `worse` |
| `segment-index-get-multisegment-hot:getMissSync` | `2692383.337 ops/s` | `3513302.350 ops/s` | `+30.49%` | `better` |
| `segment-index-get-overlay:getHitAsyncJoin` | `251708.580 ops/s` | `180260.448 ops/s` | `-28.39%` | `worse` |
| `segment-index-get-overlay:getHitSync` | `2746525.157 ops/s` | `3727323.153 ops/s` | `+35.71%` | `better` |
| `segment-index-get-overlay:getMissAsyncJoin` | `255605.754 ops/s` | `180728.928 ops/s` | `-29.29%` | `worse` |
| `segment-index-get-overlay:getMissSync` | `2536393.076 ops/s` | `3626607.918 ops/s` | `+42.98%` | `better` |
| `segment-index-get-persisted:getHitAsyncJoin` | `77850.648 ops/s` | `66323.493 ops/s` | `-14.81%` | `worse` |
| `segment-index-get-persisted:getHitSync` | `114724.420 ops/s` | `112718.281 ops/s` | `-1.75%` | `neutral` |
| `segment-index-get-persisted:getMissAsyncJoin` | `256772.514 ops/s` | `186291.075 ops/s` | `-27.45%` | `worse` |
| `segment-index-get-persisted:getMissSync` | `2636366.929 ops/s` | `3605450.238 ops/s` | `+36.76%` | `better` |
| `segment-index-hot-partition-put:putHotPartition` | `1811828.413 ops/s` | `2568871.245 ops/s` | `+41.78%` | `better` |
| `segment-index-hot-partition-put:putThenGetHotPartition` | `1084697.537 ops/s` | `1407159.384 ops/s` | `+29.73%` | `better` |
| `segment-index-lifecycle:openAndCheckAndRepairConsistency` | `134.044 ms/op` | `277.869 ms/op` | `+107.30%` | `better` |
| `segment-index-lifecycle:openAndCompact` | `154.930 ms/op` | `299.121 ms/op` | `+93.07%` | `better` |
| `segment-index-lifecycle:openExisting` | `131.867 ms/op` | `273.855 ms/op` | `+107.67%` | `better` |
| `segment-index-mixed-drain:partitionedIngestMixed` | `458651.873 ops/s` | `511622.560 ops/s` | `+11.55%` | `better` |
| `segment-index-mixed-drain:partitionedIngestMixed:getWorkload` | `453254.282 ops/s` | `506225.314 ops/s` | `+11.69%` | `better` |
| `segment-index-mixed-drain:partitionedIngestMixed:putWorkload` | `5397.591 ops/s` | `5397.246 ops/s` | `-0.01%` | `neutral` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed` | `265928.341 ops/s` | `261729.683 ops/s` | `-1.58%` | `neutral` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:getWorkload` | `264609.262 ops/s` | `260260.418 ops/s` | `-1.64%` | `neutral` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:putWorkload` | `1319.079 ops/s` | `1469.265 ops/s` | `+11.39%` | `better` |
| `segment-index-persisted-mutation:deleteAsyncJoin` | `1816.593 ops/s` | `2513.217 ops/s` | `+38.35%` | `better` |
| `segment-index-persisted-mutation:deleteSync` | `2031.903 ops/s` | `2777.465 ops/s` | `+36.69%` | `better` |
| `segment-index-persisted-mutation:putAsyncJoin` | `1869.422 ops/s` | `2468.463 ops/s` | `+32.04%` | `better` |
| `segment-index-persisted-mutation:putSync` | `1862.816 ops/s` | `2758.963 ops/s` | `+48.11%` | `better` |
| `single-chunk-entry-write-compact:writeEntrySteadyState` | `8943333.893 ops/s` | `8286342.521 ops/s` | `-7.35%` | `worse` |
| `single-chunk-entry-write-large:writeEntrySteadyState` | `8030959.646 ops/s` | `7798420.045 ops/s` | `-2.90%` | `neutral` |
| `sorted-data-diff-key-read-compact:readNextKey` | `7984079.222 ops/s` | `8550848.010 ops/s` | `+7.10%` | `better` |
| `sorted-data-diff-key-read-large:readNextKey` | `7122824.695 ops/s` | `6678947.451 ops/s` | `-6.23%` | `warning` |
