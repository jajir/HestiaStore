# Benchmark Comparison

- Profile: `segment-index-nightly`
- Baseline SHA: `6da25f8d2edfcb35fc090cb0bcf177026066b7a3`
- Candidate SHA: `6da25f8d2edfcb35fc090cb0bcf177026066b7a3`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `segment-index-get-live:getHitSync` | `6617236.941 ops/s` | `6820716.371 ops/s` | `+3.07%` | `better` |
| `segment-index-get-live:getMissSync` | `6952214.681 ops/s` | `6356205.694 ops/s` | `-8.57%` | `worse` |
| `segment-index-get-multisegment-cold:getHitSync` | `473551.721 ops/s` | `481532.939 ops/s` | `+1.69%` | `neutral` |
| `segment-index-get-multisegment-cold:getMissSync` | `6650979.343 ops/s` | `5808433.713 ops/s` | `-12.67%` | `worse` |
| `segment-index-get-multisegment-hot:getHitSync` | `5635503.132 ops/s` | `5657704.647 ops/s` | `+0.39%` | `neutral` |
| `segment-index-get-multisegment-hot:getMissSync` | `5972192.582 ops/s` | `7066919.614 ops/s` | `+18.33%` | `better` |
| `segment-index-get-persisted:getHitSync` | `4657792.957 ops/s` | `5052348.544 ops/s` | `+8.47%` | `better` |
| `segment-index-get-persisted:getMissSync` | `6712350.590 ops/s` | `6110161.049 ops/s` | `-8.97%` | `worse` |
| `segment-index-hot-route-put:putHotRoute` | `6267664.385 ops/s` | `5733591.650 ops/s` | `-8.52%` | `worse` |
| `segment-index-hot-route-put:putThenGetHotRoute` | `3180008.224 ops/s` | `3194252.726 ops/s` | `+0.45%` | `neutral` |
| `segment-index-lifecycle:openAndCheckAndRepairConsistency` | `202.845 ms/op` | `207.231 ms/op` | `+2.16%` | `neutral` |
| `segment-index-lifecycle:openAndCompact` | `210.378 ms/op` | `213.764 ms/op` | `+1.61%` | `neutral` |
| `segment-index-lifecycle:openExisting` | `201.100 ms/op` | `201.337 ms/op` | `+0.12%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed` | `735343.389 ops/s` | `705624.520 ops/s` | `-4.04%` | `warning` |
| `segment-index-mixed-drain:partitionedIngestMixed:getWorkload` | `448016.361 ops/s` | `422581.967 ops/s` | `-5.68%` | `warning` |
| `segment-index-mixed-drain:partitionedIngestMixed:putWorkload` | `287327.028 ops/s` | `283042.554 ops/s` | `-1.49%` | `neutral` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed` | `1920407.115 ops/s` | `2049791.437 ops/s` | `+6.74%` | `better` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:getWorkload` | `1869529.368 ops/s` | `2000406.013 ops/s` | `+7.00%` | `better` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:putWorkload` | `50877.747 ops/s` | `49385.424 ops/s` | `-2.93%` | `neutral` |
| `segment-index-persisted-mutation-concurrent:deleteSync` | `5486.989 ops/s` | `7527.253 ops/s` | `+37.18%` | `better` |
| `segment-index-persisted-mutation-concurrent:putSync` | `5684.213 ops/s` | `7819.647 ops/s` | `+37.57%` | `better` |
| `segment-index-persisted-mutation:deleteSync` | `3268.726 ops/s` | `3632.762 ops/s` | `+11.14%` | `better` |
| `segment-index-persisted-mutation:putSync` | `2897.999 ops/s` | `3504.981 ops/s` | `+20.94%` | `better` |
| `segment-index-range-scan:boundedScan` | `17.101 us/op` | `18.600 us/op` | `+8.77%` | `better` |
| `segment-index-range-scan:fullStreamRangeFallback` | `1157.448 us/op` | `1299.096 us/op` | `+12.24%` | `better` |
| `segment-index-range-scan:sequentialRead` | `2263.074 us/op` | `2517.285 us/op` | `+11.23%` | `better` |
| `segment-merge-sequential:mergeSequential` | `175.718 us/op` | `191.620 us/op` | `+9.05%` | `better` |
