# Benchmark Comparison

- Candidate labels rerun 3x total and median-merged because the initial regression exceeded threshold with high relative score error: `segment-index-get-persisted,segment-index-lifecycle`


- Profile: `segment-index-nightly`
- Baseline SHA: `e72f4974ea597aa48d2725b131d13e39bcfa6872`
- Candidate SHA: `e72f4974ea597aa48d2725b131d13e39bcfa6872`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `segment-index-get-multisegment-cold:getHitAsyncJoin` | `41.452 ops/s` | `41.329 ops/s` | `-0.30%` | `neutral` |
| `segment-index-get-multisegment-cold:getHitSync` | `37.096 ops/s` | `56.471 ops/s` | `+52.23%` | `better` |
| `segment-index-get-multisegment-cold:getMissAsyncJoin` | `183449.991 ops/s` | `169811.025 ops/s` | `-7.43%` | `worse` |
| `segment-index-get-multisegment-cold:getMissSync` | `3625724.192 ops/s` | `3762303.936 ops/s` | `+3.77%` | `better` |
| `segment-index-get-multisegment-hot:getHitAsyncJoin` | `90.642 ops/s` | `91.590 ops/s` | `+1.05%` | `neutral` |
| `segment-index-get-multisegment-hot:getHitSync` | `96.257 ops/s` | `99.323 ops/s` | `+3.19%` | `better` |
| `segment-index-get-multisegment-hot:getMissAsyncJoin` | `185356.908 ops/s` | `170666.767 ops/s` | `-7.93%` | `worse` |
| `segment-index-get-multisegment-hot:getMissSync` | `4029504.501 ops/s` | `3625872.320 ops/s` | `-10.02%` | `worse` |
| `segment-index-get-overlay:getHitAsyncJoin` | `182574.140 ops/s` | `162613.482 ops/s` | `-10.93%` | `worse` |
| `segment-index-get-overlay:getHitSync` | `3902628.246 ops/s` | `3966777.304 ops/s` | `+1.64%` | `neutral` |
| `segment-index-get-overlay:getMissAsyncJoin` | `186894.452 ops/s` | `154865.730 ops/s` | `-17.14%` | `worse` |
| `segment-index-get-overlay:getMissSync` | `3664701.561 ops/s` | `3969731.610 ops/s` | `+8.32%` | `better` |
| `segment-index-get-persisted:getHitAsyncJoin` | `69279.352 ops/s` | `63647.181 ops/s` | `-8.13%` | `worse` |
| `segment-index-get-persisted:getHitSync` | `111991.450 ops/s` | `115634.294 ops/s` | `+3.25%` | `better` |
| `segment-index-get-persisted:getMissAsyncJoin` | `185301.358 ops/s` | `163590.753 ops/s` | `-11.72%` | `worse` |
| `segment-index-get-persisted:getMissSync` | `3908382.680 ops/s` | `4068153.285 ops/s` | `+4.09%` | `better` |
| `segment-index-hot-partition-put:putHotPartition` | `2663749.952 ops/s` | `3008574.489 ops/s` | `+12.95%` | `better` |
| `segment-index-hot-partition-put:putThenGetHotPartition` | `1605512.887 ops/s` | `1620752.632 ops/s` | `+0.95%` | `neutral` |
| `segment-index-lifecycle:openAndCheckAndRepairConsistency` | `278.921 ms/op` | `247.027 ms/op` | `-11.43%` | `worse` |
| `segment-index-lifecycle:openAndCompact` | `301.308 ms/op` | `267.149 ms/op` | `-11.34%` | `worse` |
| `segment-index-lifecycle:openExisting` | `274.682 ms/op` | `246.006 ms/op` | `-10.44%` | `worse` |
| `segment-index-mixed-drain:partitionedIngestMixed` | `512433.489 ops/s` | `526659.295 ops/s` | `+2.78%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed:getWorkload` | `507056.666 ops/s` | `521357.919 ops/s` | `+2.82%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed:putWorkload` | `5376.822 ops/s` | `5301.376 ops/s` | `-1.40%` | `neutral` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed` | `256807.948 ops/s` | `257983.101 ops/s` | `+0.46%` | `neutral` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:getWorkload` | `255240.742 ops/s` | `256611.834 ops/s` | `+0.54%` | `neutral` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:putWorkload` | `1567.206 ops/s` | `1371.267 ops/s` | `-12.50%` | `worse` |
| `segment-index-persisted-mutation:deleteAsyncJoin` | `2392.134 ops/s` | `2033.800 ops/s` | `-14.98%` | `worse` |
| `segment-index-persisted-mutation:deleteSync` | `2547.035 ops/s` | `2264.646 ops/s` | `-11.09%` | `worse` |
| `segment-index-persisted-mutation:putAsyncJoin` | `2359.298 ops/s` | `1940.969 ops/s` | `-17.73%` | `worse` |
| `segment-index-persisted-mutation:putSync` | `2554.118 ops/s` | `2176.208 ops/s` | `-14.80%` | `worse` |
| `single-chunk-entry-write-compact:writeEntrySteadyState` | `8224327.181 ops/s` | `8552802.049 ops/s` | `+3.99%` | `better` |
| `single-chunk-entry-write-large:writeEntrySteadyState` | `7697762.220 ops/s` | `7939654.697 ops/s` | `+3.14%` | `better` |
| `sorted-data-diff-key-read-compact:readNextKey` | `7745263.446 ops/s` | `8640934.183 ops/s` | `+11.56%` | `better` |
| `sorted-data-diff-key-read-large:readNextKey` | `6722420.531 ops/s` | `6963478.464 ops/s` | `+3.59%` | `better` |
