# Benchmark Comparison

- Candidate labels rerun 3x total and median-merged because the initial regression exceeded threshold with high relative score error: `segment-index-mixed-split-heavy`


- Profile: `segment-index-nightly`
- Baseline SHA: `e72f4974ea597aa48d2725b131d13e39bcfa6872`
- Candidate SHA: `e72f4974ea597aa48d2725b131d13e39bcfa6872`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `segment-index-get-multisegment-cold:getHitAsyncJoin` | `45.037 ops/s` | `44.858 ops/s` | `-0.40%` | `neutral` |
| `segment-index-get-multisegment-cold:getHitSync` | `43.521 ops/s` | `46.454 ops/s` | `+6.74%` | `better` |
| `segment-index-get-multisegment-cold:getMissAsyncJoin` | `170761.681 ops/s` | `170995.247 ops/s` | `+0.14%` | `neutral` |
| `segment-index-get-multisegment-cold:getMissSync` | `3689382.510 ops/s` | `3602333.240 ops/s` | `-2.36%` | `neutral` |
| `segment-index-get-multisegment-hot:getHitAsyncJoin` | `82.315 ops/s` | `90.899 ops/s` | `+10.43%` | `better` |
| `segment-index-get-multisegment-hot:getHitSync` | `88.310 ops/s` | `89.008 ops/s` | `+0.79%` | `neutral` |
| `segment-index-get-multisegment-hot:getMissAsyncJoin` | `168972.374 ops/s` | `172106.983 ops/s` | `+1.86%` | `neutral` |
| `segment-index-get-multisegment-hot:getMissSync` | `3729614.593 ops/s` | `3658338.670 ops/s` | `-1.91%` | `neutral` |
| `segment-index-get-overlay:getHitAsyncJoin` | `160192.186 ops/s` | `154233.267 ops/s` | `-3.72%` | `warning` |
| `segment-index-get-overlay:getHitSync` | `3822420.043 ops/s` | `4112327.116 ops/s` | `+7.58%` | `better` |
| `segment-index-get-overlay:getMissAsyncJoin` | `162963.962 ops/s` | `166342.814 ops/s` | `+2.07%` | `neutral` |
| `segment-index-get-overlay:getMissSync` | `3925160.617 ops/s` | `3699481.502 ops/s` | `-5.75%` | `warning` |
| `segment-index-get-persisted:getHitAsyncJoin` | `60868.196 ops/s` | `63619.991 ops/s` | `+4.52%` | `better` |
| `segment-index-get-persisted:getHitSync` | `114944.213 ops/s` | `115793.282 ops/s` | `+0.74%` | `neutral` |
| `segment-index-get-persisted:getMissAsyncJoin` | `164812.017 ops/s` | `160839.781 ops/s` | `-2.41%` | `neutral` |
| `segment-index-get-persisted:getMissSync` | `3762276.370 ops/s` | `3827199.676 ops/s` | `+1.73%` | `neutral` |
| `segment-index-hot-partition-put:putHotPartition` | `3019337.790 ops/s` | `3018352.345 ops/s` | `-0.03%` | `neutral` |
| `segment-index-hot-partition-put:putThenGetHotPartition` | `1688793.418 ops/s` | `1630754.796 ops/s` | `-3.44%` | `warning` |
| `segment-index-lifecycle:openAndCheckAndRepairConsistency` | `246.450 ms/op` | `246.202 ms/op` | `-0.10%` | `neutral` |
| `segment-index-lifecycle:openAndCompact` | `268.094 ms/op` | `268.270 ms/op` | `+0.07%` | `neutral` |
| `segment-index-lifecycle:openExisting` | `244.533 ms/op` | `243.149 ms/op` | `-0.57%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed` | `495566.823 ops/s` | `521470.364 ops/s` | `+5.23%` | `better` |
| `segment-index-mixed-drain:partitionedIngestMixed:getWorkload` | `490199.499 ops/s` | `516165.955 ops/s` | `+5.30%` | `better` |
| `segment-index-mixed-drain:partitionedIngestMixed:putWorkload` | `5367.325 ops/s` | `5304.410 ops/s` | `-1.17%` | `neutral` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed` | `306037.392 ops/s` | `271278.749 ops/s` | `-11.36%` | `worse` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:getWorkload` | `259977.126 ops/s` | `264211.272 ops/s` | `+1.63%` | `neutral` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:putWorkload` | `46060.267 ops/s` | `1520.983 ops/s` | `-96.70%` | `worse` |
| `segment-index-persisted-mutation:deleteAsyncJoin` | `2028.569 ops/s` | `2100.731 ops/s` | `+3.56%` | `better` |
| `segment-index-persisted-mutation:deleteSync` | `2281.802 ops/s` | `2287.295 ops/s` | `+0.24%` | `neutral` |
| `segment-index-persisted-mutation:putAsyncJoin` | `2010.657 ops/s` | `2034.758 ops/s` | `+1.20%` | `neutral` |
| `segment-index-persisted-mutation:putSync` | `2239.315 ops/s` | `2217.413 ops/s` | `-0.98%` | `neutral` |
| `single-chunk-entry-write-compact:writeEntrySteadyState` | `8501399.426 ops/s` | `8461280.408 ops/s` | `-0.47%` | `neutral` |
| `single-chunk-entry-write-large:writeEntrySteadyState` | `7952282.319 ops/s` | `7498331.852 ops/s` | `-5.71%` | `warning` |
| `sorted-data-diff-key-read-compact:readNextKey` | `8633835.358 ops/s` | `8690269.476 ops/s` | `+0.65%` | `neutral` |
| `sorted-data-diff-key-read-large:readNextKey` | `7432550.234 ops/s` | `7069965.756 ops/s` | `-4.88%` | `warning` |
