# Benchmark Comparison

- Candidate labels rerun 3x total and median-merged because the initial regression exceeded threshold with high relative score error: `segment-index-get-multisegment-hot,segment-index-get-overlay,segment-index-get-persisted,segment-index-mixed-split-heavy`


- Profile: `segment-index-nightly`
- Baseline SHA: `e72f4974ea597aa48d2725b131d13e39bcfa6872`
- Candidate SHA: `e72f4974ea597aa48d2725b131d13e39bcfa6872`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `segment-index-get-multisegment-cold:getHitAsyncJoin` | `54.032 ops/s` | `55.454 ops/s` | `+2.63%` | `neutral` |
| `segment-index-get-multisegment-cold:getHitSync` | `38.605 ops/s` | `44.636 ops/s` | `+15.62%` | `better` |
| `segment-index-get-multisegment-cold:getMissAsyncJoin` | `167641.298 ops/s` | `247718.846 ops/s` | `+47.77%` | `better` |
| `segment-index-get-multisegment-cold:getMissSync` | `3597563.854 ops/s` | `2287404.152 ops/s` | `-36.42%` | `worse` |
| `segment-index-get-multisegment-hot:getHitAsyncJoin` | `95.429 ops/s` | `87.663 ops/s` | `-8.14%` | `worse` |
| `segment-index-get-multisegment-hot:getHitSync` | `89.916 ops/s` | `94.299 ops/s` | `+4.87%` | `better` |
| `segment-index-get-multisegment-hot:getMissAsyncJoin` | `168028.701 ops/s` | `246537.529 ops/s` | `+46.72%` | `better` |
| `segment-index-get-multisegment-hot:getMissSync` | `3587021.755 ops/s` | `2387100.071 ops/s` | `-33.45%` | `worse` |
| `segment-index-get-overlay:getHitAsyncJoin` | `159643.937 ops/s` | `241896.679 ops/s` | `+51.52%` | `better` |
| `segment-index-get-overlay:getHitSync` | `3985592.079 ops/s` | `2642190.265 ops/s` | `-33.71%` | `worse` |
| `segment-index-get-overlay:getMissAsyncJoin` | `161112.748 ops/s` | `243916.273 ops/s` | `+51.39%` | `better` |
| `segment-index-get-overlay:getMissSync` | `3683736.361 ops/s` | `2344716.011 ops/s` | `-36.35%` | `worse` |
| `segment-index-get-persisted:getHitAsyncJoin` | `57593.471 ops/s` | `73593.140 ops/s` | `+27.78%` | `better` |
| `segment-index-get-persisted:getHitSync` | `115434.722 ops/s` | `112634.868 ops/s` | `-2.43%` | `neutral` |
| `segment-index-get-persisted:getMissAsyncJoin` | `163553.947 ops/s` | `229285.697 ops/s` | `+40.19%` | `better` |
| `segment-index-get-persisted:getMissSync` | `3704959.205 ops/s` | `2407490.468 ops/s` | `-35.02%` | `worse` |
| `segment-index-hot-partition-put:putHotPartition` | `2734767.609 ops/s` | `1854809.375 ops/s` | `-32.18%` | `worse` |
| `segment-index-hot-partition-put:putThenGetHotPartition` | `1657761.100 ops/s` | `1021990.314 ops/s` | `-38.35%` | `worse` |
| `segment-index-lifecycle:openAndCheckAndRepairConsistency` | `249.049 ms/op` | `136.720 ms/op` | `-45.10%` | `worse` |
| `segment-index-lifecycle:openAndCompact` | `271.475 ms/op` | `156.392 ms/op` | `-42.39%` | `worse` |
| `segment-index-lifecycle:openExisting` | `246.168 ms/op` | `132.746 ms/op` | `-46.08%` | `worse` |
| `segment-index-mixed-drain:partitionedIngestMixed` | `490104.772 ops/s` | `469621.268 ops/s` | `-4.18%` | `warning` |
| `segment-index-mixed-drain:partitionedIngestMixed:getWorkload` | `484775.813 ops/s` | `464248.896 ops/s` | `-4.23%` | `warning` |
| `segment-index-mixed-drain:partitionedIngestMixed:putWorkload` | `5328.959 ops/s` | `5372.372 ops/s` | `+0.81%` | `neutral` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed` | `316711.176 ops/s` | `280232.879 ops/s` | `-11.52%` | `worse` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:getWorkload` | `263547.305 ops/s` | `257374.467 ops/s` | `-2.34%` | `neutral` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:putWorkload` | `53163.871 ops/s` | `22858.412 ops/s` | `-57.00%` | `worse` |
| `segment-index-persisted-mutation:deleteAsyncJoin` | `1970.968 ops/s` | `1987.219 ops/s` | `+0.82%` | `neutral` |
| `segment-index-persisted-mutation:deleteSync` | `2124.232 ops/s` | `2070.007 ops/s` | `-2.55%` | `neutral` |
| `segment-index-persisted-mutation:putAsyncJoin` | `1899.660 ops/s` | `1694.332 ops/s` | `-10.81%` | `worse` |
| `segment-index-persisted-mutation:putSync` | `2059.382 ops/s` | `1979.453 ops/s` | `-3.88%` | `warning` |
| `single-chunk-entry-write-compact:writeEntrySteadyState` | `8434797.215 ops/s` | `8871261.733 ops/s` | `+5.17%` | `better` |
| `single-chunk-entry-write-large:writeEntrySteadyState` | `7820212.013 ops/s` | `7891548.307 ops/s` | `+0.91%` | `neutral` |
| `sorted-data-diff-key-read-compact:readNextKey` | `8567140.421 ops/s` | `8158585.756 ops/s` | `-4.77%` | `warning` |
| `sorted-data-diff-key-read-large:readNextKey` | `6989408.013 ops/s` | `7122211.305 ops/s` | `+1.90%` | `neutral` |
