# Benchmark Comparison

- Profile: `segment-index-nightly`
- Baseline SHA: `6da25f8d2edfcb35fc090cb0bcf177026066b7a3`
- Candidate SHA: `6da25f8d2edfcb35fc090cb0bcf177026066b7a3`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `segment-index-get-live:getHitSync` | `5330470.480 ops/s` | `5769757.064 ops/s` | `+8.24%` | `better` |
| `segment-index-get-live:getMissSync` | `4518270.404 ops/s` | `5267203.493 ops/s` | `+16.58%` | `better` |
| `segment-index-get-multisegment-cold:getHitSync` | `273846.817 ops/s` | `337089.129 ops/s` | `+23.09%` | `better` |
| `segment-index-get-multisegment-cold:getMissSync` | `4576067.311 ops/s` | `5274814.165 ops/s` | `+15.27%` | `better` |
| `segment-index-get-multisegment-hot:getHitSync` | `3627008.601 ops/s` | `3567885.962 ops/s` | `-1.63%` | `neutral` |
| `segment-index-get-multisegment-hot:getMissSync` | `4575331.797 ops/s` | `5403268.404 ops/s` | `+18.10%` | `better` |
| `segment-index-get-persisted:getHitSync` | `3229149.011 ops/s` | `3776705.941 ops/s` | `+16.96%` | `better` |
| `segment-index-get-persisted:getMissSync` | `4771545.877 ops/s` | `5031382.830 ops/s` | `+5.45%` | `better` |
| `segment-index-hot-route-put:putHotRoute` | `4478846.662 ops/s` | `4769706.354 ops/s` | `+6.49%` | `better` |
| `segment-index-hot-route-put:putThenGetHotRoute` | `2333173.471 ops/s` | `2407040.344 ops/s` | `+3.17%` | `better` |
| `segment-index-lifecycle:openAndCheckAndRepairConsistency` | `237.804 ms/op` | `226.156 ms/op` | `-4.90%` | `warning` |
| `segment-index-lifecycle:openAndCompact` | `255.266 ms/op` | `250.087 ms/op` | `-2.03%` | `neutral` |
| `segment-index-lifecycle:openExisting` | `237.804 ms/op` | `225.536 ms/op` | `-5.16%` | `warning` |
| `segment-index-mixed-drain:partitionedIngestMixed` | `576296.345 ops/s` | `635145.619 ops/s` | `+10.21%` | `better` |
| `segment-index-mixed-drain:partitionedIngestMixed:getWorkload` | `274303.089 ops/s` | `323569.462 ops/s` | `+17.96%` | `better` |
| `segment-index-mixed-drain:partitionedIngestMixed:putWorkload` | `301993.256 ops/s` | `311576.157 ops/s` | `+3.17%` | `better` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed` | `1287846.497 ops/s` | `1349068.876 ops/s` | `+4.75%` | `better` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:getWorkload` | `1253655.838 ops/s` | `1306457.912 ops/s` | `+4.21%` | `better` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:putWorkload` | `34190.659 ops/s` | `42610.963 ops/s` | `+24.63%` | `better` |
| `segment-index-persisted-mutation-concurrent:deleteSync` | `8908.289 ops/s` | `5391.501 ops/s` | `-39.48%` | `worse` |
| `segment-index-persisted-mutation-concurrent:putSync` | `8860.223 ops/s` | `3214.108 ops/s` | `-63.72%` | `worse` |
| `segment-index-persisted-mutation:deleteSync` | `3178.742 ops/s` | `1566.814 ops/s` | `-50.71%` | `worse` |
| `segment-index-persisted-mutation:putSync` | `3150.647 ops/s` | `1563.815 ops/s` | `-50.37%` | `worse` |
| `segment-index-range-scan:boundedScan` | `28.026 us/op` | `23.611 us/op` | `-15.75%` | `worse` |
| `segment-index-range-scan:fullStreamRangeFallback` | `1973.586 us/op` | `1687.448 us/op` | `-14.50%` | `worse` |
| `segment-index-range-scan:sequentialRead` | `3881.458 us/op` | `3360.070 us/op` | `-13.43%` | `worse` |
| `segment-merge-sequential:mergeSequential` | `332.591 us/op` | `307.003 us/op` | `-7.69%` | `worse` |
