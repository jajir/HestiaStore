# Benchmark Comparison

- Candidate labels rerun 3x total and median-merged because the initial regression exceeded threshold with high relative score error: `segment-index-get-multisegment-cold,segment-index-lifecycle`


- Profile: `segment-index-nightly`
- Baseline SHA: `e72f4974ea597aa48d2725b131d13e39bcfa6872`
- Candidate SHA: `e72f4974ea597aa48d2725b131d13e39bcfa6872`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `segment-index-get-multisegment-cold:getHitAsyncJoin` | `50.313 ops/s` | `48.076 ops/s` | `-4.44%` | `warning` |
| `segment-index-get-multisegment-cold:getHitSync` | `41.827 ops/s` | `42.265 ops/s` | `+1.05%` | `neutral` |
| `segment-index-get-multisegment-cold:getMissAsyncJoin` | `184638.280 ops/s` | `172630.973 ops/s` | `-6.50%` | `warning` |
| `segment-index-get-multisegment-cold:getMissSync` | `3368826.431 ops/s` | `3808545.198 ops/s` | `+13.05%` | `better` |
| `segment-index-get-multisegment-hot:getHitAsyncJoin` | `85.442 ops/s` | `90.798 ops/s` | `+6.27%` | `better` |
| `segment-index-get-multisegment-hot:getHitSync` | `88.910 ops/s` | `85.625 ops/s` | `-3.69%` | `warning` |
| `segment-index-get-multisegment-hot:getMissAsyncJoin` | `183948.880 ops/s` | `176820.683 ops/s` | `-3.88%` | `warning` |
| `segment-index-get-multisegment-hot:getMissSync` | `3359494.042 ops/s` | `3851355.615 ops/s` | `+14.64%` | `better` |
| `segment-index-get-overlay:getHitAsyncJoin` | `182926.205 ops/s` | `160354.565 ops/s` | `-12.34%` | `worse` |
| `segment-index-get-overlay:getHitSync` | `3651023.379 ops/s` | `3753593.906 ops/s` | `+2.81%` | `neutral` |
| `segment-index-get-overlay:getMissAsyncJoin` | `183403.242 ops/s` | `166333.514 ops/s` | `-9.31%` | `worse` |
| `segment-index-get-overlay:getMissSync` | `3634300.663 ops/s` | `3959238.208 ops/s` | `+8.94%` | `better` |
| `segment-index-get-persisted:getHitAsyncJoin` | `59975.896 ops/s` | `65048.937 ops/s` | `+8.46%` | `better` |
| `segment-index-get-persisted:getHitSync` | `111627.095 ops/s` | `116439.361 ops/s` | `+4.31%` | `better` |
| `segment-index-get-persisted:getMissAsyncJoin` | `179253.381 ops/s` | `162219.844 ops/s` | `-9.50%` | `worse` |
| `segment-index-get-persisted:getMissSync` | `3666550.553 ops/s` | `4001931.338 ops/s` | `+9.15%` | `better` |
| `segment-index-hot-partition-put:putHotPartition` | `2928244.976 ops/s` | `2785688.485 ops/s` | `-4.87%` | `warning` |
| `segment-index-hot-partition-put:putThenGetHotPartition` | `1497552.137 ops/s` | `1622753.130 ops/s` | `+8.36%` | `better` |
| `segment-index-lifecycle:openAndCheckAndRepairConsistency` | `278.349 ms/op` | `245.655 ms/op` | `-11.75%` | `worse` |
| `segment-index-lifecycle:openAndCompact` | `301.710 ms/op` | `264.605 ms/op` | `-12.30%` | `worse` |
| `segment-index-lifecycle:openExisting` | `273.406 ms/op` | `242.707 ms/op` | `-11.23%` | `worse` |
| `segment-index-mixed-drain:partitionedIngestMixed` | `507486.747 ops/s` | `509439.270 ops/s` | `+0.38%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed:getWorkload` | `502193.166 ops/s` | `504103.140 ops/s` | `+0.38%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed:putWorkload` | `5293.580 ops/s` | `5336.130 ops/s` | `+0.80%` | `neutral` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed` | `258170.148 ops/s` | `273441.358 ops/s` | `+5.92%` | `better` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:getWorkload` | `256694.315 ops/s` | `271896.567 ops/s` | `+5.92%` | `better` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:putWorkload` | `1475.834 ops/s` | `1544.792 ops/s` | `+4.67%` | `better` |
| `segment-index-persisted-mutation:deleteAsyncJoin` | `2546.258 ops/s` | `2173.599 ops/s` | `-14.64%` | `worse` |
| `segment-index-persisted-mutation:deleteSync` | `2807.389 ops/s` | `2371.324 ops/s` | `-15.53%` | `worse` |
| `segment-index-persisted-mutation:putAsyncJoin` | `2513.389 ops/s` | `2001.809 ops/s` | `-20.35%` | `worse` |
| `segment-index-persisted-mutation:putSync` | `2775.332 ops/s` | `2260.010 ops/s` | `-18.57%` | `worse` |
| `single-chunk-entry-write-compact:writeEntrySteadyState` | `8285100.741 ops/s` | `8678918.985 ops/s` | `+4.75%` | `better` |
| `single-chunk-entry-write-large:writeEntrySteadyState` | `7851361.353 ops/s` | `7979965.392 ops/s` | `+1.64%` | `neutral` |
| `sorted-data-diff-key-read-compact:readNextKey` | `8525035.009 ops/s` | `9528113.624 ops/s` | `+11.77%` | `better` |
| `sorted-data-diff-key-read-large:readNextKey` | `6710109.783 ops/s` | `7557163.862 ops/s` | `+12.62%` | `better` |
