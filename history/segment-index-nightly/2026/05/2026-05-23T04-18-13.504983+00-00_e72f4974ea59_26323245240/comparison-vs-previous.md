# Benchmark Comparison

- Candidate labels rerun 3x total and median-merged because the initial regression exceeded threshold with high relative score error: `segment-index-get-multisegment-cold`


- Profile: `segment-index-nightly`
- Baseline SHA: `e72f4974ea597aa48d2725b131d13e39bcfa6872`
- Candidate SHA: `e72f4974ea597aa48d2725b131d13e39bcfa6872`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `segment-index-get-multisegment-cold:getHitAsyncJoin` | `41.463 ops/s` | `46.849 ops/s` | `+12.99%` | `better` |
| `segment-index-get-multisegment-cold:getHitSync` | `48.185 ops/s` | `40.873 ops/s` | `-15.17%` | `worse` |
| `segment-index-get-multisegment-cold:getMissAsyncJoin` | `175238.433 ops/s` | `187777.801 ops/s` | `+7.16%` | `better` |
| `segment-index-get-multisegment-cold:getMissSync` | `3901727.445 ops/s` | `3723652.000 ops/s` | `-4.56%` | `warning` |
| `segment-index-get-multisegment-hot:getHitAsyncJoin` | `101.419 ops/s` | `98.955 ops/s` | `-2.43%` | `neutral` |
| `segment-index-get-multisegment-hot:getHitSync` | `95.302 ops/s` | `93.858 ops/s` | `-1.52%` | `neutral` |
| `segment-index-get-multisegment-hot:getMissAsyncJoin` | `173634.006 ops/s` | `188124.509 ops/s` | `+8.35%` | `better` |
| `segment-index-get-multisegment-hot:getMissSync` | `3790990.815 ops/s` | `3783549.573 ops/s` | `-0.20%` | `neutral` |
| `segment-index-get-overlay:getHitAsyncJoin` | `166443.513 ops/s` | `182119.914 ops/s` | `+9.42%` | `better` |
| `segment-index-get-overlay:getHitSync` | `4152178.532 ops/s` | `3474248.398 ops/s` | `-16.33%` | `worse` |
| `segment-index-get-overlay:getMissAsyncJoin` | `169262.324 ops/s` | `186513.904 ops/s` | `+10.19%` | `better` |
| `segment-index-get-overlay:getMissSync` | `4428039.527 ops/s` | `3614659.006 ops/s` | `-18.37%` | `worse` |
| `segment-index-get-persisted:getHitAsyncJoin` | `61024.361 ops/s` | `61820.529 ops/s` | `+1.30%` | `neutral` |
| `segment-index-get-persisted:getHitSync` | `118513.450 ops/s` | `114937.484 ops/s` | `-3.02%` | `warning` |
| `segment-index-get-persisted:getMissAsyncJoin` | `164648.594 ops/s` | `182198.502 ops/s` | `+10.66%` | `better` |
| `segment-index-get-persisted:getMissSync` | `3668708.157 ops/s` | `3422769.085 ops/s` | `-6.70%` | `warning` |
| `segment-index-hot-partition-put:putHotPartition` | `3177279.925 ops/s` | `2923653.934 ops/s` | `-7.98%` | `worse` |
| `segment-index-hot-partition-put:putThenGetHotPartition` | `1618454.573 ops/s` | `1602974.855 ops/s` | `-0.96%` | `neutral` |
| `segment-index-lifecycle:openAndCheckAndRepairConsistency` | `247.410 ms/op` | `277.298 ms/op` | `+12.08%` | `better` |
| `segment-index-lifecycle:openAndCompact` | `267.961 ms/op` | `297.932 ms/op` | `+11.18%` | `better` |
| `segment-index-lifecycle:openExisting` | `244.785 ms/op` | `274.555 ms/op` | `+12.16%` | `better` |
| `segment-index-mixed-drain:partitionedIngestMixed` | `512491.868 ops/s` | `516494.624 ops/s` | `+0.78%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed:getWorkload` | `507177.522 ops/s` | `511083.374 ops/s` | `+0.77%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed:putWorkload` | `5314.346 ops/s` | `5411.251 ops/s` | `+1.82%` | `neutral` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed` | `272648.760 ops/s` | `263479.891 ops/s` | `-3.36%` | `warning` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:getWorkload` | `256785.544 ops/s` | `262065.240 ops/s` | `+2.06%` | `neutral` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:putWorkload` | `1516.842 ops/s` | `1414.651 ops/s` | `-6.74%` | `warning` |
| `segment-index-persisted-mutation:deleteAsyncJoin` | `2060.168 ops/s` | `2559.516 ops/s` | `+24.24%` | `better` |
| `segment-index-persisted-mutation:deleteSync` | `2293.262 ops/s` | `2796.787 ops/s` | `+21.96%` | `better` |
| `segment-index-persisted-mutation:putAsyncJoin` | `2010.847 ops/s` | `2509.645 ops/s` | `+24.81%` | `better` |
| `segment-index-persisted-mutation:putSync` | `2248.926 ops/s` | `2669.734 ops/s` | `+18.71%` | `better` |
| `single-chunk-entry-write-compact:writeEntrySteadyState` | `8573825.565 ops/s` | `8281588.335 ops/s` | `-3.41%` | `warning` |
| `single-chunk-entry-write-large:writeEntrySteadyState` | `7890789.325 ops/s` | `7535485.853 ops/s` | `-4.50%` | `warning` |
| `sorted-data-diff-key-read-compact:readNextKey` | `8841772.508 ops/s` | `8603633.352 ops/s` | `-2.69%` | `neutral` |
| `sorted-data-diff-key-read-large:readNextKey` | `7469610.391 ops/s` | `6754916.559 ops/s` | `-9.57%` | `worse` |
