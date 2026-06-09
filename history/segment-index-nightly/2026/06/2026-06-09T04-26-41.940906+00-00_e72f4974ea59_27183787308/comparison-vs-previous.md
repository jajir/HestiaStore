# Benchmark Comparison

- Candidate labels rerun 3x total and median-merged because the initial regression exceeded threshold with high relative score error: `segment-index-get-multisegment-cold,segment-index-get-multisegment-hot,segment-index-lifecycle`


- Profile: `segment-index-nightly`
- Baseline SHA: `e72f4974ea597aa48d2725b131d13e39bcfa6872`
- Candidate SHA: `e72f4974ea597aa48d2725b131d13e39bcfa6872`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `segment-index-get-multisegment-cold:getHitAsyncJoin` | `51.129 ops/s` | `37.701 ops/s` | `-26.26%` | `worse` |
| `segment-index-get-multisegment-cold:getHitSync` | `51.329 ops/s` | `40.677 ops/s` | `-20.75%` | `worse` |
| `segment-index-get-multisegment-cold:getMissAsyncJoin` | `183966.385 ops/s` | `170329.078 ops/s` | `-7.41%` | `worse` |
| `segment-index-get-multisegment-cold:getMissSync` | `3836626.677 ops/s` | `3764721.953 ops/s` | `-1.87%` | `neutral` |
| `segment-index-get-multisegment-hot:getHitAsyncJoin` | `103.448 ops/s` | `101.639 ops/s` | `-1.75%` | `neutral` |
| `segment-index-get-multisegment-hot:getHitSync` | `100.357 ops/s` | `87.356 ops/s` | `-12.95%` | `worse` |
| `segment-index-get-multisegment-hot:getMissAsyncJoin` | `185948.038 ops/s` | `168672.793 ops/s` | `-9.29%` | `worse` |
| `segment-index-get-multisegment-hot:getMissSync` | `3776749.121 ops/s` | `3653770.465 ops/s` | `-3.26%` | `warning` |
| `segment-index-get-overlay:getHitAsyncJoin` | `179081.049 ops/s` | `160811.679 ops/s` | `-10.20%` | `worse` |
| `segment-index-get-overlay:getHitSync` | `3491744.503 ops/s` | `4118118.810 ops/s` | `+17.94%` | `better` |
| `segment-index-get-overlay:getMissAsyncJoin` | `187288.686 ops/s` | `163169.442 ops/s` | `-12.88%` | `worse` |
| `segment-index-get-overlay:getMissSync` | `3765874.173 ops/s` | `3541675.725 ops/s` | `-5.95%` | `warning` |
| `segment-index-get-persisted:getHitAsyncJoin` | `63153.664 ops/s` | `58128.692 ops/s` | `-7.96%` | `worse` |
| `segment-index-get-persisted:getHitSync` | `114357.581 ops/s` | `116788.510 ops/s` | `+2.13%` | `neutral` |
| `segment-index-get-persisted:getMissAsyncJoin` | `179642.320 ops/s` | `163600.894 ops/s` | `-8.93%` | `worse` |
| `segment-index-get-persisted:getMissSync` | `3493042.803 ops/s` | `3510297.820 ops/s` | `+0.49%` | `neutral` |
| `segment-index-hot-partition-put:putHotPartition` | `2683150.631 ops/s` | `3032029.964 ops/s` | `+13.00%` | `better` |
| `segment-index-hot-partition-put:putThenGetHotPartition` | `1448324.938 ops/s` | `1662194.791 ops/s` | `+14.77%` | `better` |
| `segment-index-lifecycle:openAndCheckAndRepairConsistency` | `278.817 ms/op` | `248.098 ms/op` | `-11.02%` | `worse` |
| `segment-index-lifecycle:openAndCompact` | `300.150 ms/op` | `269.766 ms/op` | `-10.12%` | `worse` |
| `segment-index-lifecycle:openExisting` | `275.502 ms/op` | `246.549 ms/op` | `-10.51%` | `worse` |
| `segment-index-mixed-drain:partitionedIngestMixed` | `516989.773 ops/s` | `495190.281 ops/s` | `-4.22%` | `warning` |
| `segment-index-mixed-drain:partitionedIngestMixed:getWorkload` | `511611.375 ops/s` | `489855.222 ops/s` | `-4.25%` | `warning` |
| `segment-index-mixed-drain:partitionedIngestMixed:putWorkload` | `5378.398 ops/s` | `5335.059 ops/s` | `-0.81%` | `neutral` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed` | `267962.011 ops/s` | `274501.939 ops/s` | `+2.44%` | `neutral` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:getWorkload` | `266483.074 ops/s` | `273093.613 ops/s` | `+2.48%` | `neutral` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:putWorkload` | `1478.937 ops/s` | `1408.326 ops/s` | `-4.77%` | `warning` |
| `segment-index-persisted-mutation:deleteAsyncJoin` | `2514.484 ops/s` | `2009.664 ops/s` | `-20.08%` | `worse` |
| `segment-index-persisted-mutation:deleteSync` | `2732.674 ops/s` | `2208.609 ops/s` | `-19.18%` | `worse` |
| `segment-index-persisted-mutation:putAsyncJoin` | `2438.330 ops/s` | `1999.608 ops/s` | `-17.99%` | `worse` |
| `segment-index-persisted-mutation:putSync` | `2706.670 ops/s` | `2109.167 ops/s` | `-22.08%` | `worse` |
| `single-chunk-entry-write-compact:writeEntrySteadyState` | `8265493.242 ops/s` | `8494585.359 ops/s` | `+2.77%` | `neutral` |
| `single-chunk-entry-write-large:writeEntrySteadyState` | `7738461.386 ops/s` | `7875744.961 ops/s` | `+1.77%` | `neutral` |
| `sorted-data-diff-key-read-compact:readNextKey` | `8614189.403 ops/s` | `9399959.239 ops/s` | `+9.12%` | `better` |
| `sorted-data-diff-key-read-large:readNextKey` | `6696807.836 ops/s` | `7434329.919 ops/s` | `+11.01%` | `better` |
