# Benchmark Comparison

- Candidate labels rerun 3x total and median-merged because the initial regression exceeded threshold with high relative score error: `segment-index-get-multisegment-cold,segment-index-persisted-mutation`


- Profile: `segment-index-nightly`
- Baseline SHA: `e72f4974ea597aa48d2725b131d13e39bcfa6872`
- Candidate SHA: `e72f4974ea597aa48d2725b131d13e39bcfa6872`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `segment-index-get-multisegment-cold:getHitAsyncJoin` | `45.847 ops/s` | `44.049 ops/s` | `-3.92%` | `warning` |
| `segment-index-get-multisegment-cold:getHitSync` | `47.682 ops/s` | `37.493 ops/s` | `-21.37%` | `worse` |
| `segment-index-get-multisegment-cold:getMissAsyncJoin` | `170896.942 ops/s` | `235880.691 ops/s` | `+38.03%` | `better` |
| `segment-index-get-multisegment-cold:getMissSync` | `3579209.297 ops/s` | `4660940.362 ops/s` | `+30.22%` | `better` |
| `segment-index-get-multisegment-hot:getHitAsyncJoin` | `95.536 ops/s` | `89.495 ops/s` | `-6.32%` | `warning` |
| `segment-index-get-multisegment-hot:getHitSync` | `93.554 ops/s` | `94.822 ops/s` | `+1.36%` | `neutral` |
| `segment-index-get-multisegment-hot:getMissAsyncJoin` | `172196.216 ops/s` | `238474.304 ops/s` | `+38.49%` | `better` |
| `segment-index-get-multisegment-hot:getMissSync` | `3636512.921 ops/s` | `4740936.568 ops/s` | `+30.37%` | `better` |
| `segment-index-get-overlay:getHitAsyncJoin` | `161025.484 ops/s` | `235316.352 ops/s` | `+46.14%` | `better` |
| `segment-index-get-overlay:getHitSync` | `3775546.589 ops/s` | `4542117.578 ops/s` | `+20.30%` | `better` |
| `segment-index-get-overlay:getMissAsyncJoin` | `163864.691 ops/s` | `238023.127 ops/s` | `+45.26%` | `better` |
| `segment-index-get-overlay:getMissSync` | `3768262.983 ops/s` | `4087352.383 ops/s` | `+8.47%` | `better` |
| `segment-index-get-persisted:getHitAsyncJoin` | `62763.848 ops/s` | `71989.881 ops/s` | `+14.70%` | `better` |
| `segment-index-get-persisted:getHitSync` | `115807.950 ops/s` | `158932.503 ops/s` | `+37.24%` | `better` |
| `segment-index-get-persisted:getMissAsyncJoin` | `166146.586 ops/s` | `234161.295 ops/s` | `+40.94%` | `better` |
| `segment-index-get-persisted:getMissSync` | `3693468.360 ops/s` | `4418976.383 ops/s` | `+19.64%` | `better` |
| `segment-index-hot-partition-put:putHotPartition` | `3141057.705 ops/s` | `3635838.264 ops/s` | `+15.75%` | `better` |
| `segment-index-hot-partition-put:putThenGetHotPartition` | `1641531.231 ops/s` | `1970228.788 ops/s` | `+20.02%` | `better` |
| `segment-index-lifecycle:openAndCheckAndRepairConsistency` | `241.602 ms/op` | `218.384 ms/op` | `-9.61%` | `worse` |
| `segment-index-lifecycle:openAndCompact` | `264.745 ms/op` | `232.611 ms/op` | `-12.14%` | `worse` |
| `segment-index-lifecycle:openExisting` | `243.309 ms/op` | `215.507 ms/op` | `-11.43%` | `worse` |
| `segment-index-mixed-drain:partitionedIngestMixed` | `551537.200 ops/s` | `776792.675 ops/s` | `+40.84%` | `better` |
| `segment-index-mixed-drain:partitionedIngestMixed:getWorkload` | `546202.672 ops/s` | `771420.822 ops/s` | `+41.23%` | `better` |
| `segment-index-mixed-drain:partitionedIngestMixed:putWorkload` | `5334.528 ops/s` | `5371.853 ops/s` | `+0.70%` | `neutral` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed` | `271333.502 ops/s` | `381139.999 ops/s` | `+40.47%` | `better` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:getWorkload` | `269822.497 ops/s` | `379603.898 ops/s` | `+40.69%` | `better` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:putWorkload` | `1511.006 ops/s` | `1536.102 ops/s` | `+1.66%` | `neutral` |
| `segment-index-persisted-mutation:deleteAsyncJoin` | `2072.729 ops/s` | `397.539 ops/s` | `-80.82%` | `worse` |
| `segment-index-persisted-mutation:deleteSync` | `2309.552 ops/s` | `490.260 ops/s` | `-78.77%` | `worse` |
| `segment-index-persisted-mutation:putAsyncJoin` | `2037.805 ops/s` | `775.997 ops/s` | `-61.92%` | `worse` |
| `segment-index-persisted-mutation:putSync` | `2239.875 ops/s` | `529.760 ops/s` | `-76.35%` | `worse` |
| `single-chunk-entry-write-compact:writeEntrySteadyState` | `8570023.213 ops/s` | `11059637.577 ops/s` | `+29.05%` | `better` |
| `single-chunk-entry-write-large:writeEntrySteadyState` | `7976521.724 ops/s` | `10192809.952 ops/s` | `+27.79%` | `better` |
| `sorted-data-diff-key-read-compact:readNextKey` | `9221189.282 ops/s` | `10819823.112 ops/s` | `+17.34%` | `better` |
| `sorted-data-diff-key-read-large:readNextKey` | `7365461.011 ops/s` | `8570061.666 ops/s` | `+16.35%` | `better` |
