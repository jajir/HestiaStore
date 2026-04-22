# Benchmark Comparison

- Candidate labels rerun 3x total and median-merged because the initial regression exceeded threshold with high relative score error: `segment-index-get-multisegment-cold,segment-index-get-multisegment-hot`


- Profile: `segment-index-nightly`
- Baseline SHA: `e72f4974ea597aa48d2725b131d13e39bcfa6872`
- Candidate SHA: `e72f4974ea597aa48d2725b131d13e39bcfa6872`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `segment-index-get-multisegment-cold:getHitAsyncJoin` | `39.688 ops/s` | `41.108 ops/s` | `+3.58%` | `better` |
| `segment-index-get-multisegment-cold:getHitSync` | `41.492 ops/s` | `34.813 ops/s` | `-16.10%` | `worse` |
| `segment-index-get-multisegment-cold:getMissAsyncJoin` | `172280.988 ops/s` | `183639.437 ops/s` | `+6.59%` | `better` |
| `segment-index-get-multisegment-cold:getMissSync` | `3788045.074 ops/s` | `3652180.135 ops/s` | `-3.59%` | `warning` |
| `segment-index-get-multisegment-hot:getHitAsyncJoin` | `96.060 ops/s` | `89.578 ops/s` | `-6.75%` | `warning` |
| `segment-index-get-multisegment-hot:getHitSync` | `89.034 ops/s` | `89.028 ops/s` | `-0.01%` | `neutral` |
| `segment-index-get-multisegment-hot:getMissAsyncJoin` | `174035.108 ops/s` | `184764.518 ops/s` | `+6.17%` | `better` |
| `segment-index-get-multisegment-hot:getMissSync` | `4110898.319 ops/s` | `3503615.462 ops/s` | `-14.77%` | `worse` |
| `segment-index-get-overlay:getHitAsyncJoin` | `156786.029 ops/s` | `187301.668 ops/s` | `+19.46%` | `better` |
| `segment-index-get-overlay:getHitSync` | `4168576.415 ops/s` | `3720416.817 ops/s` | `-10.75%` | `worse` |
| `segment-index-get-overlay:getMissAsyncJoin` | `156286.985 ops/s` | `183567.874 ops/s` | `+17.46%` | `better` |
| `segment-index-get-overlay:getMissSync` | `3789194.220 ops/s` | `3681268.980 ops/s` | `-2.85%` | `neutral` |
| `segment-index-get-persisted:getHitAsyncJoin` | `62056.903 ops/s` | `61557.356 ops/s` | `-0.80%` | `neutral` |
| `segment-index-get-persisted:getHitSync` | `115044.253 ops/s` | `116213.083 ops/s` | `+1.02%` | `neutral` |
| `segment-index-get-persisted:getMissAsyncJoin` | `164924.963 ops/s` | `179074.071 ops/s` | `+8.58%` | `better` |
| `segment-index-get-persisted:getMissSync` | `3875849.368 ops/s` | `3921952.015 ops/s` | `+1.19%` | `neutral` |
| `segment-index-hot-partition-put:putHotPartition` | `2815830.466 ops/s` | `2412956.578 ops/s` | `-14.31%` | `worse` |
| `segment-index-hot-partition-put:putThenGetHotPartition` | `1669428.286 ops/s` | `1593379.698 ops/s` | `-4.56%` | `warning` |
| `segment-index-lifecycle:openAndCheckAndRepairConsistency` | `243.557 ms/op` | `277.820 ms/op` | `+14.07%` | `better` |
| `segment-index-lifecycle:openAndCompact` | `264.710 ms/op` | `301.582 ms/op` | `+13.93%` | `better` |
| `segment-index-lifecycle:openExisting` | `241.316 ms/op` | `274.155 ms/op` | `+13.61%` | `better` |
| `segment-index-mixed-drain:partitionedIngestMixed` | `493513.460 ops/s` | `492877.968 ops/s` | `-0.13%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed:getWorkload` | `488145.421 ops/s` | `487520.091 ops/s` | `-0.13%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed:putWorkload` | `5368.039 ops/s` | `5357.877 ops/s` | `-0.19%` | `neutral` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed` | `263364.183 ops/s` | `263327.840 ops/s` | `-0.01%` | `neutral` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:getWorkload` | `261873.515 ops/s` | `261880.357 ops/s` | `+0.00%` | `neutral` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:putWorkload` | `1490.668 ops/s` | `1447.483 ops/s` | `-2.90%` | `neutral` |
| `segment-index-persisted-mutation:deleteAsyncJoin` | `1990.078 ops/s` | `2494.425 ops/s` | `+25.34%` | `better` |
| `segment-index-persisted-mutation:deleteSync` | `2146.076 ops/s` | `2619.607 ops/s` | `+22.06%` | `better` |
| `segment-index-persisted-mutation:putAsyncJoin` | `1913.809 ops/s` | `2393.316 ops/s` | `+25.06%` | `better` |
| `segment-index-persisted-mutation:putSync` | `2136.706 ops/s` | `2581.500 ops/s` | `+20.82%` | `better` |
| `single-chunk-entry-write-compact:writeEntrySteadyState` | `8513860.599 ops/s` | `8337781.085 ops/s` | `-2.07%` | `neutral` |
| `single-chunk-entry-write-large:writeEntrySteadyState` | `7954295.350 ops/s` | `7845520.562 ops/s` | `-1.37%` | `neutral` |
| `sorted-data-diff-key-read-compact:readNextKey` | `9435940.265 ops/s` | `8551319.074 ops/s` | `-9.38%` | `worse` |
| `sorted-data-diff-key-read-large:readNextKey` | `7482597.459 ops/s` | `6669595.393 ops/s` | `-10.87%` | `worse` |
