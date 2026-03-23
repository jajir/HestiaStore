# Benchmark Comparison

- Candidate labels rerun 3x total and median-merged because the initial regression exceeded threshold with high relative score error: `segment-index-get-multisegment-cold,segment-index-get-multisegment-hot`


- Profile: `segment-index-nightly`
- Baseline SHA: `e6adc34214c9ff0a3d8eae146bfacb8b07282732`
- Candidate SHA: `9d5a339a22a9b41105834c8ae9ce3405bf76481c`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `segment-index-get-multisegment-cold:getHitAsyncJoin` | `42.001 ops/s` | `40.003 ops/s` | `-4.76%` | `warning` |
| `segment-index-get-multisegment-cold:getHitSync` | `45.434 ops/s` | `40.542 ops/s` | `-10.77%` | `worse` |
| `segment-index-get-multisegment-cold:getMissAsyncJoin` | `174671.016 ops/s` | `174362.865 ops/s` | `-0.18%` | `neutral` |
| `segment-index-get-multisegment-cold:getMissSync` | `4097606.352 ops/s` | `3508647.508 ops/s` | `-14.37%` | `worse` |
| `segment-index-get-multisegment-hot:getHitAsyncJoin` | `94.461 ops/s` | `87.507 ops/s` | `-7.36%` | `worse` |
| `segment-index-get-multisegment-hot:getHitSync` | `91.810 ops/s` | `90.828 ops/s` | `-1.07%` | `neutral` |
| `segment-index-get-multisegment-hot:getMissAsyncJoin` | `173505.797 ops/s` | `174199.776 ops/s` | `+0.40%` | `neutral` |
| `segment-index-get-multisegment-hot:getMissSync` | `3383942.905 ops/s` | `3597809.461 ops/s` | `+6.32%` | `better` |
| `segment-index-get-overlay:getHitAsyncJoin` | `171035.151 ops/s` | `169377.048 ops/s` | `-0.97%` | `neutral` |
| `segment-index-get-overlay:getHitSync` | `4000323.717 ops/s` | `4258298.392 ops/s` | `+6.45%` | `better` |
| `segment-index-get-overlay:getMissAsyncJoin` | `166898.630 ops/s` | `168994.658 ops/s` | `+1.26%` | `neutral` |
| `segment-index-get-overlay:getMissSync` | `3532029.943 ops/s` | `4508556.042 ops/s` | `+27.65%` | `better` |
| `segment-index-get-persisted:getHitAsyncJoin` | `61058.770 ops/s` | `61498.761 ops/s` | `+0.72%` | `neutral` |
| `segment-index-get-persisted:getHitSync` | `112222.702 ops/s` | `111111.245 ops/s` | `-0.99%` | `neutral` |
| `segment-index-get-persisted:getMissAsyncJoin` | `167543.479 ops/s` | `171467.385 ops/s` | `+2.34%` | `neutral` |
| `segment-index-get-persisted:getMissSync` | `3697065.270 ops/s` | `3655973.577 ops/s` | `-1.11%` | `neutral` |
| `segment-index-hot-partition-put:putHotPartition` | `2824655.665 ops/s` | `2964478.679 ops/s` | `+4.95%` | `better` |
| `segment-index-hot-partition-put:putThenGetHotPartition` | `1519937.444 ops/s` | `1700066.708 ops/s` | `+11.85%` | `better` |
| `segment-index-lifecycle:openAndCheckAndRepairConsistency` | `307.980 ms/op` | `307.366 ms/op` | `-0.20%` | `neutral` |
| `segment-index-lifecycle:openAndCompact` | `328.086 ms/op` | `330.870 ms/op` | `+0.85%` | `neutral` |
| `segment-index-lifecycle:openExisting` | `303.111 ms/op` | `304.530 ms/op` | `+0.47%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed` | `505849.718 ops/s` | `493395.286 ops/s` | `-2.46%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed:getWorkload` | `500466.760 ops/s` | `488027.157 ops/s` | `-2.49%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed:putWorkload` | `5382.958 ops/s` | `5368.129 ops/s` | `-0.28%` | `neutral` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed` | `271823.876 ops/s` | `268251.105 ops/s` | `-1.31%` | `neutral` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:getWorkload` | `270331.336 ops/s` | `266839.460 ops/s` | `-1.29%` | `neutral` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:putWorkload` | `1492.540 ops/s` | `1411.644 ops/s` | `-5.42%` | `warning` |
| `segment-index-persisted-mutation:deleteAsyncJoin` | `1818.928 ops/s` | `1794.957 ops/s` | `-1.32%` | `neutral` |
| `segment-index-persisted-mutation:deleteSync` | `1974.779 ops/s` | `1961.699 ops/s` | `-0.66%` | `neutral` |
| `segment-index-persisted-mutation:putAsyncJoin` | `1769.614 ops/s` | `1759.673 ops/s` | `-0.56%` | `neutral` |
| `segment-index-persisted-mutation:putSync` | `1901.741 ops/s` | `1927.228 ops/s` | `+1.34%` | `neutral` |
