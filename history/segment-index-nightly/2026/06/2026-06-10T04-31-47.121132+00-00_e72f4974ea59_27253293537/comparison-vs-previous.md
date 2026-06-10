# Benchmark Comparison

- Candidate labels rerun 3x total and median-merged because the initial regression exceeded threshold with high relative score error: `segment-index-get-multisegment-cold,segment-index-get-multisegment-hot`


- Profile: `segment-index-nightly`
- Baseline SHA: `e72f4974ea597aa48d2725b131d13e39bcfa6872`
- Candidate SHA: `e72f4974ea597aa48d2725b131d13e39bcfa6872`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `segment-index-get-multisegment-cold:getHitAsyncJoin` | `37.701 ops/s` | `48.239 ops/s` | `+27.95%` | `better` |
| `segment-index-get-multisegment-cold:getHitSync` | `40.677 ops/s` | `48.211 ops/s` | `+18.52%` | `better` |
| `segment-index-get-multisegment-cold:getMissAsyncJoin` | `170329.078 ops/s` | `170588.728 ops/s` | `+0.15%` | `neutral` |
| `segment-index-get-multisegment-cold:getMissSync` | `3764721.953 ops/s` | `3440505.341 ops/s` | `-8.61%` | `worse` |
| `segment-index-get-multisegment-hot:getHitAsyncJoin` | `101.639 ops/s` | `89.401 ops/s` | `-12.04%` | `worse` |
| `segment-index-get-multisegment-hot:getHitSync` | `87.356 ops/s` | `81.308 ops/s` | `-6.92%` | `warning` |
| `segment-index-get-multisegment-hot:getMissAsyncJoin` | `168672.793 ops/s` | `170900.228 ops/s` | `+1.32%` | `neutral` |
| `segment-index-get-multisegment-hot:getMissSync` | `3653770.465 ops/s` | `3773860.600 ops/s` | `+3.29%` | `better` |
| `segment-index-get-overlay:getHitAsyncJoin` | `160811.679 ops/s` | `161652.025 ops/s` | `+0.52%` | `neutral` |
| `segment-index-get-overlay:getHitSync` | `4118118.810 ops/s` | `3942425.109 ops/s` | `-4.27%` | `warning` |
| `segment-index-get-overlay:getMissAsyncJoin` | `163169.442 ops/s` | `162420.144 ops/s` | `-0.46%` | `neutral` |
| `segment-index-get-overlay:getMissSync` | `3541675.725 ops/s` | `3791248.716 ops/s` | `+7.05%` | `better` |
| `segment-index-get-persisted:getHitAsyncJoin` | `58128.692 ops/s` | `58289.388 ops/s` | `+0.28%` | `neutral` |
| `segment-index-get-persisted:getHitSync` | `116788.510 ops/s` | `112651.746 ops/s` | `-3.54%` | `warning` |
| `segment-index-get-persisted:getMissAsyncJoin` | `163600.894 ops/s` | `163855.031 ops/s` | `+0.16%` | `neutral` |
| `segment-index-get-persisted:getMissSync` | `3510297.820 ops/s` | `3680590.953 ops/s` | `+4.85%` | `better` |
| `segment-index-hot-partition-put:putHotPartition` | `3032029.964 ops/s` | `2972182.750 ops/s` | `-1.97%` | `neutral` |
| `segment-index-hot-partition-put:putThenGetHotPartition` | `1662194.791 ops/s` | `1549335.691 ops/s` | `-6.79%` | `warning` |
| `segment-index-lifecycle:openAndCheckAndRepairConsistency` | `248.098 ms/op` | `243.581 ms/op` | `-1.82%` | `neutral` |
| `segment-index-lifecycle:openAndCompact` | `269.766 ms/op` | `264.236 ms/op` | `-2.05%` | `neutral` |
| `segment-index-lifecycle:openExisting` | `246.549 ms/op` | `240.020 ms/op` | `-2.65%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed` | `495190.281 ops/s` | `518649.353 ops/s` | `+4.74%` | `better` |
| `segment-index-mixed-drain:partitionedIngestMixed:getWorkload` | `489855.222 ops/s` | `513296.158 ops/s` | `+4.79%` | `better` |
| `segment-index-mixed-drain:partitionedIngestMixed:putWorkload` | `5335.059 ops/s` | `5353.195 ops/s` | `+0.34%` | `neutral` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed` | `274501.939 ops/s` | `257764.132 ops/s` | `-6.10%` | `warning` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:getWorkload` | `273093.613 ops/s` | `250222.915 ops/s` | `-8.37%` | `worse` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:putWorkload` | `1408.326 ops/s` | `7541.218 ops/s` | `+435.47%` | `better` |
| `segment-index-persisted-mutation:deleteAsyncJoin` | `2009.664 ops/s` | `2028.192 ops/s` | `+0.92%` | `neutral` |
| `segment-index-persisted-mutation:deleteSync` | `2208.609 ops/s` | `2279.408 ops/s` | `+3.21%` | `better` |
| `segment-index-persisted-mutation:putAsyncJoin` | `1999.608 ops/s` | `2016.157 ops/s` | `+0.83%` | `neutral` |
| `segment-index-persisted-mutation:putSync` | `2109.167 ops/s` | `2241.161 ops/s` | `+6.26%` | `better` |
| `single-chunk-entry-write-compact:writeEntrySteadyState` | `8494585.359 ops/s` | `8458660.006 ops/s` | `-0.42%` | `neutral` |
| `single-chunk-entry-write-large:writeEntrySteadyState` | `7875744.961 ops/s` | `7867101.302 ops/s` | `-0.11%` | `neutral` |
| `sorted-data-diff-key-read-compact:readNextKey` | `9399959.239 ops/s` | `9488783.183 ops/s` | `+0.94%` | `neutral` |
| `sorted-data-diff-key-read-large:readNextKey` | `7434329.919 ops/s` | `7415244.473 ops/s` | `-0.26%` | `neutral` |
