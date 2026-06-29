# Benchmark Comparison

- Candidate labels rerun 3x total and median-merged because the initial regression exceeded threshold with high relative score error: `segment-index-get-multisegment-cold`


- Profile: `segment-index-nightly`
- Baseline SHA: `e72f4974ea597aa48d2725b131d13e39bcfa6872`
- Candidate SHA: `e72f4974ea597aa48d2725b131d13e39bcfa6872`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `segment-index-get-multisegment-cold:getHitAsyncJoin` | `46.048 ops/s` | `38.029 ops/s` | `-17.41%` | `worse` |
| `segment-index-get-multisegment-cold:getHitSync` | `44.627 ops/s` | `43.350 ops/s` | `-2.86%` | `neutral` |
| `segment-index-get-multisegment-cold:getMissAsyncJoin` | `184578.724 ops/s` | `184233.796 ops/s` | `-0.19%` | `neutral` |
| `segment-index-get-multisegment-cold:getMissSync` | `3652743.000 ops/s` | `3527578.430 ops/s` | `-3.43%` | `warning` |
| `segment-index-get-multisegment-hot:getHitAsyncJoin` | `100.428 ops/s` | `97.078 ops/s` | `-3.34%` | `warning` |
| `segment-index-get-multisegment-hot:getHitSync` | `105.653 ops/s` | `102.549 ops/s` | `-2.94%` | `neutral` |
| `segment-index-get-multisegment-hot:getMissAsyncJoin` | `184366.002 ops/s` | `186532.711 ops/s` | `+1.18%` | `neutral` |
| `segment-index-get-multisegment-hot:getMissSync` | `3577133.221 ops/s` | `3555973.410 ops/s` | `-0.59%` | `neutral` |
| `segment-index-get-overlay:getHitAsyncJoin` | `182653.643 ops/s` | `183281.494 ops/s` | `+0.34%` | `neutral` |
| `segment-index-get-overlay:getHitSync` | `3367967.483 ops/s` | `3624551.534 ops/s` | `+7.62%` | `better` |
| `segment-index-get-overlay:getMissAsyncJoin` | `182661.646 ops/s` | `184255.232 ops/s` | `+0.87%` | `neutral` |
| `segment-index-get-overlay:getMissSync` | `3514883.136 ops/s` | `3548517.937 ops/s` | `+0.96%` | `neutral` |
| `segment-index-get-persisted:getHitAsyncJoin` | `67675.328 ops/s` | `63547.566 ops/s` | `-6.10%` | `warning` |
| `segment-index-get-persisted:getHitSync` | `110159.849 ops/s` | `115317.127 ops/s` | `+4.68%` | `better` |
| `segment-index-get-persisted:getMissAsyncJoin` | `179648.956 ops/s` | `185779.443 ops/s` | `+3.41%` | `better` |
| `segment-index-get-persisted:getMissSync` | `3366995.872 ops/s` | `3514106.064 ops/s` | `+4.37%` | `better` |
| `segment-index-hot-partition-put:putHotPartition` | `2874835.078 ops/s` | `2614533.503 ops/s` | `-9.05%` | `worse` |
| `segment-index-hot-partition-put:putThenGetHotPartition` | `1656156.558 ops/s` | `1523709.651 ops/s` | `-8.00%` | `worse` |
| `segment-index-lifecycle:openAndCheckAndRepairConsistency` | `279.822 ms/op` | `288.308 ms/op` | `+3.03%` | `better` |
| `segment-index-lifecycle:openAndCompact` | `300.809 ms/op` | `299.391 ms/op` | `-0.47%` | `neutral` |
| `segment-index-lifecycle:openExisting` | `275.725 ms/op` | `274.867 ms/op` | `-0.31%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed` | `505436.130 ops/s` | `515498.174 ops/s` | `+1.99%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed:getWorkload` | `500053.730 ops/s` | `510160.354 ops/s` | `+2.02%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed:putWorkload` | `5382.399 ops/s` | `5337.820 ops/s` | `-0.83%` | `neutral` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed` | `266233.655 ops/s` | `264502.037 ops/s` | `-0.65%` | `neutral` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:getWorkload` | `264766.011 ops/s` | `263019.773 ops/s` | `-0.66%` | `neutral` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:putWorkload` | `1467.644 ops/s` | `1482.263 ops/s` | `+1.00%` | `neutral` |
| `segment-index-persisted-mutation:deleteAsyncJoin` | `2492.965 ops/s` | `2485.038 ops/s` | `-0.32%` | `neutral` |
| `segment-index-persisted-mutation:deleteSync` | `2720.308 ops/s` | `2769.009 ops/s` | `+1.79%` | `neutral` |
| `segment-index-persisted-mutation:putAsyncJoin` | `2415.769 ops/s` | `2512.670 ops/s` | `+4.01%` | `better` |
| `segment-index-persisted-mutation:putSync` | `2697.550 ops/s` | `2711.908 ops/s` | `+0.53%` | `neutral` |
| `single-chunk-entry-write-compact:writeEntrySteadyState` | `8297125.269 ops/s` | `8305015.487 ops/s` | `+0.10%` | `neutral` |
| `single-chunk-entry-write-large:writeEntrySteadyState` | `7741783.359 ops/s` | `7776176.449 ops/s` | `+0.44%` | `neutral` |
| `sorted-data-diff-key-read-compact:readNextKey` | `8562725.407 ops/s` | `8590061.683 ops/s` | `+0.32%` | `neutral` |
| `sorted-data-diff-key-read-large:readNextKey` | `6800997.440 ops/s` | `6754364.607 ops/s` | `-0.69%` | `neutral` |
