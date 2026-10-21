# Benchmark Comparison

- Profile: `segment-index-nightly`
- Baseline SHA: `6da25f8d2edfcb35fc090cb0bcf177026066b7a3`
- Candidate SHA: `6da25f8d2edfcb35fc090cb0bcf177026066b7a3`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `segment-index-get-live:getHitSync` | `5012443.535 ops/s` | `5904932.626 ops/s` | `+17.81%` | `better` |
| `segment-index-get-live:getMissSync` | `4678745.618 ops/s` | `5937353.412 ops/s` | `+26.90%` | `better` |
| `segment-index-get-multisegment-cold:getHitSync` | `281650.649 ops/s` | `378566.642 ops/s` | `+34.41%` | `better` |
| `segment-index-get-multisegment-cold:getMissSync` | `4900198.072 ops/s` | `5607592.569 ops/s` | `+14.44%` | `better` |
| `segment-index-get-multisegment-hot:getHitSync` | `3679481.405 ops/s` | `4316602.239 ops/s` | `+17.32%` | `better` |
| `segment-index-get-multisegment-hot:getMissSync` | `4103574.031 ops/s` | `5315837.239 ops/s` | `+29.54%` | `better` |
| `segment-index-get-persisted:getHitSync` | `3668278.597 ops/s` | `4223233.328 ops/s` | `+15.13%` | `better` |
| `segment-index-get-persisted:getMissSync` | `4895628.553 ops/s` | `5631908.676 ops/s` | `+15.04%` | `better` |
| `segment-index-hot-route-put:putHotRoute` | `4728287.748 ops/s` | `5033636.534 ops/s` | `+6.46%` | `better` |
| `segment-index-hot-route-put:putThenGetHotRoute` | `2277668.774 ops/s` | `2539760.377 ops/s` | `+11.51%` | `better` |
| `segment-index-lifecycle:openAndCheckAndRepairConsistency` | `245.750 ms/op` | `204.781 ms/op` | `-16.67%` | `worse` |
| `segment-index-lifecycle:openAndCompact` | `260.550 ms/op` | `220.589 ms/op` | `-15.34%` | `worse` |
| `segment-index-lifecycle:openExisting` | `241.618 ms/op` | `201.377 ms/op` | `-16.65%` | `worse` |
| `segment-index-mixed-drain:partitionedIngestMixed` | `538105.599 ops/s` | `693306.290 ops/s` | `+28.84%` | `better` |
| `segment-index-mixed-drain:partitionedIngestMixed:getWorkload` | `248357.031 ops/s` | `373762.825 ops/s` | `+50.49%` | `better` |
| `segment-index-mixed-drain:partitionedIngestMixed:putWorkload` | `289748.568 ops/s` | `319543.464 ops/s` | `+10.28%` | `better` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed` | `1254161.503 ops/s` | `1379354.470 ops/s` | `+9.98%` | `better` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:getWorkload` | `1220426.181 ops/s` | `1335929.324 ops/s` | `+9.46%` | `better` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:putWorkload` | `33735.322 ops/s` | `43425.146 ops/s` | `+28.72%` | `better` |
| `segment-index-persisted-mutation-concurrent:deleteSync` | `8795.036 ops/s` | `4566.401 ops/s` | `-48.08%` | `worse` |
| `segment-index-persisted-mutation-concurrent:putSync` | `8814.218 ops/s` | `5833.009 ops/s` | `-33.82%` | `worse` |
| `segment-index-persisted-mutation:deleteSync` | `3259.133 ops/s` | `2249.093 ops/s` | `-30.99%` | `worse` |
| `segment-index-persisted-mutation:putSync` | `3098.436 ops/s` | `2046.101 ops/s` | `-33.96%` | `worse` |
| `segment-index-range-scan:boundedScan` | `34.306 us/op` | `23.388 us/op` | `-31.83%` | `worse` |
| `segment-index-range-scan:fullStreamRangeFallback` | `1968.568 us/op` | `1601.854 us/op` | `-18.63%` | `worse` |
| `segment-index-range-scan:sequentialRead` | `3933.202 us/op` | `3097.760 us/op` | `-21.24%` | `worse` |
| `segment-merge-sequential:mergeSequential` | `334.567 us/op` | `279.528 us/op` | `-16.45%` | `worse` |
