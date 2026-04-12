# Benchmark Comparison

- Candidate labels rerun 3x total and median-merged because the initial regression exceeded threshold with high relative score error: `segment-index-get-multisegment-cold`


- Profile: `segment-index-nightly`
- Baseline SHA: `e72f4974ea597aa48d2725b131d13e39bcfa6872`
- Candidate SHA: `e72f4974ea597aa48d2725b131d13e39bcfa6872`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `segment-index-get-multisegment-cold:getHitAsyncJoin` | `52.910 ops/s` | `41.452 ops/s` | `-21.66%` | `worse` |
| `segment-index-get-multisegment-cold:getHitSync` | `44.302 ops/s` | `37.096 ops/s` | `-16.26%` | `worse` |
| `segment-index-get-multisegment-cold:getMissAsyncJoin` | `172366.675 ops/s` | `183449.991 ops/s` | `+6.43%` | `better` |
| `segment-index-get-multisegment-cold:getMissSync` | `3608280.445 ops/s` | `3625724.192 ops/s` | `+0.48%` | `neutral` |
| `segment-index-get-multisegment-hot:getHitAsyncJoin` | `89.482 ops/s` | `90.642 ops/s` | `+1.30%` | `neutral` |
| `segment-index-get-multisegment-hot:getHitSync` | `85.621 ops/s` | `96.257 ops/s` | `+12.42%` | `better` |
| `segment-index-get-multisegment-hot:getMissAsyncJoin` | `171596.313 ops/s` | `185356.908 ops/s` | `+8.02%` | `better` |
| `segment-index-get-multisegment-hot:getMissSync` | `3759477.487 ops/s` | `4029504.501 ops/s` | `+7.18%` | `better` |
| `segment-index-get-overlay:getHitAsyncJoin` | `164945.393 ops/s` | `182574.140 ops/s` | `+10.69%` | `better` |
| `segment-index-get-overlay:getHitSync` | `4077630.974 ops/s` | `3902628.246 ops/s` | `-4.29%` | `warning` |
| `segment-index-get-overlay:getMissAsyncJoin` | `158126.438 ops/s` | `186894.452 ops/s` | `+18.19%` | `better` |
| `segment-index-get-overlay:getMissSync` | `3638837.567 ops/s` | `3664701.561 ops/s` | `+0.71%` | `neutral` |
| `segment-index-get-persisted:getHitAsyncJoin` | `62744.968 ops/s` | `69279.352 ops/s` | `+10.41%` | `better` |
| `segment-index-get-persisted:getHitSync` | `115378.181 ops/s` | `111991.450 ops/s` | `-2.94%` | `neutral` |
| `segment-index-get-persisted:getMissAsyncJoin` | `160445.796 ops/s` | `185301.358 ops/s` | `+15.49%` | `better` |
| `segment-index-get-persisted:getMissSync` | `3972340.485 ops/s` | `3908382.680 ops/s` | `-1.61%` | `neutral` |
| `segment-index-hot-partition-put:putHotPartition` | `2999846.533 ops/s` | `2663749.952 ops/s` | `-11.20%` | `worse` |
| `segment-index-hot-partition-put:putThenGetHotPartition` | `1656008.332 ops/s` | `1605512.887 ops/s` | `-3.05%` | `warning` |
| `segment-index-lifecycle:openAndCheckAndRepairConsistency` | `244.573 ms/op` | `278.921 ms/op` | `+14.04%` | `better` |
| `segment-index-lifecycle:openAndCompact` | `266.457 ms/op` | `301.308 ms/op` | `+13.08%` | `better` |
| `segment-index-lifecycle:openExisting` | `241.544 ms/op` | `274.682 ms/op` | `+13.72%` | `better` |
| `segment-index-mixed-drain:partitionedIngestMixed` | `504348.445 ops/s` | `512433.489 ops/s` | `+1.60%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed:getWorkload` | `499015.290 ops/s` | `507056.666 ops/s` | `+1.61%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed:putWorkload` | `5333.155 ops/s` | `5376.822 ops/s` | `+0.82%` | `neutral` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed` | `266790.825 ops/s` | `256807.948 ops/s` | `-3.74%` | `warning` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:getWorkload` | `265374.657 ops/s` | `255240.742 ops/s` | `-3.82%` | `warning` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:putWorkload` | `1512.868 ops/s` | `1567.206 ops/s` | `+3.59%` | `better` |
| `segment-index-persisted-mutation:deleteAsyncJoin` | `2021.502 ops/s` | `2392.134 ops/s` | `+18.33%` | `better` |
| `segment-index-persisted-mutation:deleteSync` | `2232.959 ops/s` | `2547.035 ops/s` | `+14.07%` | `better` |
| `segment-index-persisted-mutation:putAsyncJoin` | `2001.895 ops/s` | `2359.298 ops/s` | `+17.85%` | `better` |
| `segment-index-persisted-mutation:putSync` | `2147.591 ops/s` | `2554.118 ops/s` | `+18.93%` | `better` |
| `single-chunk-entry-write-compact:writeEntrySteadyState` | `8516272.167 ops/s` | `8224327.181 ops/s` | `-3.43%` | `warning` |
| `single-chunk-entry-write-large:writeEntrySteadyState` | `7925505.610 ops/s` | `7697762.220 ops/s` | `-2.87%` | `neutral` |
| `sorted-data-diff-key-read-compact:readNextKey` | `9455176.095 ops/s` | `7745263.446 ops/s` | `-18.08%` | `worse` |
| `sorted-data-diff-key-read-large:readNextKey` | `7455649.855 ops/s` | `6722420.531 ops/s` | `-9.83%` | `worse` |
