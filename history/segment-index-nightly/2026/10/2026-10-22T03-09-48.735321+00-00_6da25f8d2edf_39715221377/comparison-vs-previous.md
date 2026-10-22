# Benchmark Comparison

- Profile: `segment-index-nightly`
- Baseline SHA: `6da25f8d2edfcb35fc090cb0bcf177026066b7a3`
- Candidate SHA: `6da25f8d2edfcb35fc090cb0bcf177026066b7a3`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `segment-index-get-live:getHitSync` | `5904932.626 ops/s` | `4771343.980 ops/s` | `-19.20%` | `worse` |
| `segment-index-get-live:getMissSync` | `5937353.412 ops/s` | `5273961.261 ops/s` | `-11.17%` | `worse` |
| `segment-index-get-multisegment-cold:getHitSync` | `378566.642 ops/s` | `270903.793 ops/s` | `-28.44%` | `worse` |
| `segment-index-get-multisegment-cold:getMissSync` | `5607592.569 ops/s` | `4592058.744 ops/s` | `-18.11%` | `worse` |
| `segment-index-get-multisegment-hot:getHitSync` | `4316602.239 ops/s` | `3736897.836 ops/s` | `-13.43%` | `worse` |
| `segment-index-get-multisegment-hot:getMissSync` | `5315837.239 ops/s` | `4606525.383 ops/s` | `-13.34%` | `worse` |
| `segment-index-get-persisted:getHitSync` | `4223233.328 ops/s` | `3272993.602 ops/s` | `-22.50%` | `worse` |
| `segment-index-get-persisted:getMissSync` | `5631908.676 ops/s` | `4453359.485 ops/s` | `-20.93%` | `worse` |
| `segment-index-hot-route-put:putHotRoute` | `5033636.534 ops/s` | `4424330.169 ops/s` | `-12.10%` | `worse` |
| `segment-index-hot-route-put:putThenGetHotRoute` | `2539760.377 ops/s` | `2149879.768 ops/s` | `-15.35%` | `worse` |
| `segment-index-lifecycle:openAndCheckAndRepairConsistency` | `204.781 ms/op` | `249.081 ms/op` | `+21.63%` | `better` |
| `segment-index-lifecycle:openAndCompact` | `220.589 ms/op` | `264.414 ms/op` | `+19.87%` | `better` |
| `segment-index-lifecycle:openExisting` | `201.377 ms/op` | `243.610 ms/op` | `+20.97%` | `better` |
| `segment-index-mixed-drain:partitionedIngestMixed` | `693306.290 ops/s` | `559550.052 ops/s` | `-19.29%` | `worse` |
| `segment-index-mixed-drain:partitionedIngestMixed:getWorkload` | `373762.825 ops/s` | `263936.274 ops/s` | `-29.38%` | `worse` |
| `segment-index-mixed-drain:partitionedIngestMixed:putWorkload` | `319543.464 ops/s` | `295613.778 ops/s` | `-7.49%` | `worse` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed` | `1379354.470 ops/s` | `1152236.622 ops/s` | `-16.47%` | `worse` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:getWorkload` | `1335929.324 ops/s` | `1116152.201 ops/s` | `-16.45%` | `worse` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:putWorkload` | `43425.146 ops/s` | `36084.421 ops/s` | `-16.90%` | `worse` |
| `segment-index-persisted-mutation-concurrent:deleteSync` | `4566.401 ops/s` | `8497.612 ops/s` | `+86.09%` | `better` |
| `segment-index-persisted-mutation-concurrent:putSync` | `5833.009 ops/s` | `8531.761 ops/s` | `+46.27%` | `better` |
| `segment-index-persisted-mutation:deleteSync` | `2249.093 ops/s` | `3264.010 ops/s` | `+45.13%` | `better` |
| `segment-index-persisted-mutation:putSync` | `2046.101 ops/s` | `3203.903 ops/s` | `+56.59%` | `better` |
| `segment-index-range-scan:boundedScan` | `23.388 us/op` | `27.014 us/op` | `+15.51%` | `better` |
| `segment-index-range-scan:fullStreamRangeFallback` | `1601.854 us/op` | `2042.002 us/op` | `+27.48%` | `better` |
| `segment-index-range-scan:sequentialRead` | `3097.760 us/op` | `3938.859 us/op` | `+27.15%` | `better` |
| `segment-merge-sequential:mergeSequential` | `279.528 us/op` | `335.301 us/op` | `+19.95%` | `better` |
