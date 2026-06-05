# Benchmark Comparison

- Candidate labels rerun 3x total and median-merged because the initial regression exceeded threshold with high relative score error: `segment-index-get-multisegment-cold,segment-index-get-multisegment-hot`


- Profile: `segment-index-nightly`
- Baseline SHA: `e72f4974ea597aa48d2725b131d13e39bcfa6872`
- Candidate SHA: `e72f4974ea597aa48d2725b131d13e39bcfa6872`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `segment-index-get-multisegment-cold:getHitAsyncJoin` | `55.454 ops/s` | `40.447 ops/s` | `-27.06%` | `worse` |
| `segment-index-get-multisegment-cold:getHitSync` | `44.636 ops/s` | `41.785 ops/s` | `-6.39%` | `warning` |
| `segment-index-get-multisegment-cold:getMissAsyncJoin` | `247718.846 ops/s` | `172318.153 ops/s` | `-30.44%` | `worse` |
| `segment-index-get-multisegment-cold:getMissSync` | `2287404.152 ops/s` | `3557712.582 ops/s` | `+55.53%` | `better` |
| `segment-index-get-multisegment-hot:getHitAsyncJoin` | `87.663 ops/s` | `84.995 ops/s` | `-3.04%` | `warning` |
| `segment-index-get-multisegment-hot:getHitSync` | `94.299 ops/s` | `86.270 ops/s` | `-8.51%` | `worse` |
| `segment-index-get-multisegment-hot:getMissAsyncJoin` | `246537.529 ops/s` | `172470.393 ops/s` | `-30.04%` | `worse` |
| `segment-index-get-multisegment-hot:getMissSync` | `2387100.071 ops/s` | `3779533.939 ops/s` | `+58.33%` | `better` |
| `segment-index-get-overlay:getHitAsyncJoin` | `241896.679 ops/s` | `165768.014 ops/s` | `-31.47%` | `worse` |
| `segment-index-get-overlay:getHitSync` | `2642190.265 ops/s` | `4346780.419 ops/s` | `+64.51%` | `better` |
| `segment-index-get-overlay:getMissAsyncJoin` | `243916.273 ops/s` | `166403.635 ops/s` | `-31.78%` | `worse` |
| `segment-index-get-overlay:getMissSync` | `2344716.011 ops/s` | `3532611.567 ops/s` | `+50.66%` | `better` |
| `segment-index-get-persisted:getHitAsyncJoin` | `73593.140 ops/s` | `63174.844 ops/s` | `-14.16%` | `worse` |
| `segment-index-get-persisted:getHitSync` | `112634.868 ops/s` | `116089.197 ops/s` | `+3.07%` | `better` |
| `segment-index-get-persisted:getMissAsyncJoin` | `229285.697 ops/s` | `165419.812 ops/s` | `-27.85%` | `worse` |
| `segment-index-get-persisted:getMissSync` | `2407490.468 ops/s` | `3798832.844 ops/s` | `+57.79%` | `better` |
| `segment-index-hot-partition-put:putHotPartition` | `1854809.375 ops/s` | `2641763.342 ops/s` | `+42.43%` | `better` |
| `segment-index-hot-partition-put:putThenGetHotPartition` | `1021990.314 ops/s` | `1651360.997 ops/s` | `+61.58%` | `better` |
| `segment-index-lifecycle:openAndCheckAndRepairConsistency` | `136.720 ms/op` | `243.782 ms/op` | `+78.31%` | `better` |
| `segment-index-lifecycle:openAndCompact` | `156.392 ms/op` | `265.507 ms/op` | `+69.77%` | `better` |
| `segment-index-lifecycle:openExisting` | `132.746 ms/op` | `241.667 ms/op` | `+82.05%` | `better` |
| `segment-index-mixed-drain:partitionedIngestMixed` | `469621.268 ops/s` | `541911.826 ops/s` | `+15.39%` | `better` |
| `segment-index-mixed-drain:partitionedIngestMixed:getWorkload` | `464248.896 ops/s` | `536596.559 ops/s` | `+15.58%` | `better` |
| `segment-index-mixed-drain:partitionedIngestMixed:putWorkload` | `5372.372 ops/s` | `5315.268 ops/s` | `-1.06%` | `neutral` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed` | `280232.879 ops/s` | `301581.415 ops/s` | `+7.62%` | `better` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:getWorkload` | `257374.467 ops/s` | `265599.401 ops/s` | `+3.20%` | `better` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:putWorkload` | `22858.412 ops/s` | `35982.014 ops/s` | `+57.41%` | `better` |
| `segment-index-persisted-mutation:deleteAsyncJoin` | `1987.219 ops/s` | `1978.203 ops/s` | `-0.45%` | `neutral` |
| `segment-index-persisted-mutation:deleteSync` | `2070.007 ops/s` | `2213.232 ops/s` | `+6.92%` | `better` |
| `segment-index-persisted-mutation:putAsyncJoin` | `1694.332 ops/s` | `1957.470 ops/s` | `+15.53%` | `better` |
| `segment-index-persisted-mutation:putSync` | `1979.453 ops/s` | `2137.225 ops/s` | `+7.97%` | `better` |
| `single-chunk-entry-write-compact:writeEntrySteadyState` | `8871261.733 ops/s` | `8335604.694 ops/s` | `-6.04%` | `warning` |
| `single-chunk-entry-write-large:writeEntrySteadyState` | `7891548.307 ops/s` | `7938132.814 ops/s` | `+0.59%` | `neutral` |
| `sorted-data-diff-key-read-compact:readNextKey` | `8158585.756 ops/s` | `9075390.755 ops/s` | `+11.24%` | `better` |
| `sorted-data-diff-key-read-large:readNextKey` | `7122211.305 ops/s` | `7413420.732 ops/s` | `+4.09%` | `better` |
