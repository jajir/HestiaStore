# Benchmark Comparison

- Profile: `segment-index-nightly`
- Baseline SHA: `6da25f8d2edfcb35fc090cb0bcf177026066b7a3`
- Candidate SHA: `6da25f8d2edfcb35fc090cb0bcf177026066b7a3`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `segment-index-get-live:getHitSync` | `3221079.754 ops/s` | `4930142.636 ops/s` | `+53.06%` | `better` |
| `segment-index-get-live:getMissSync` | `2372387.800 ops/s` | `4685283.775 ops/s` | `+97.49%` | `better` |
| `segment-index-get-multisegment-cold:getHitSync` | `317962.206 ops/s` | `353852.420 ops/s` | `+11.29%` | `better` |
| `segment-index-get-multisegment-cold:getMissSync` | `2382232.397 ops/s` | `5465775.400 ops/s` | `+129.44%` | `better` |
| `segment-index-get-multisegment-hot:getHitSync` | `1991537.459 ops/s` | `4225724.639 ops/s` | `+112.18%` | `better` |
| `segment-index-get-multisegment-hot:getMissSync` | `2976488.199 ops/s` | `5402114.525 ops/s` | `+81.49%` | `better` |
| `segment-index-get-persisted:getHitSync` | `2075155.474 ops/s` | `3322968.490 ops/s` | `+60.13%` | `better` |
| `segment-index-get-persisted:getMissSync` | `2928479.453 ops/s` | `4749856.951 ops/s` | `+62.20%` | `better` |
| `segment-index-hot-route-put:putHotRoute` | `2613336.238 ops/s` | `4410588.353 ops/s` | `+68.77%` | `better` |
| `segment-index-hot-route-put:putThenGetHotRoute` | `1354229.113 ops/s` | `2397532.993 ops/s` | `+77.04%` | `better` |
| `segment-index-lifecycle:openAndCheckAndRepairConsistency` | `110.935 ms/op` | `239.275 ms/op` | `+115.69%` | `better` |
| `segment-index-lifecycle:openAndCompact` | `125.567 ms/op` | `258.856 ms/op` | `+106.15%` | `better` |
| `segment-index-lifecycle:openExisting` | `106.369 ms/op` | `243.475 ms/op` | `+128.90%` | `better` |
| `segment-index-mixed-drain:partitionedIngestMixed` | `512821.092 ops/s` | `591609.286 ops/s` | `+15.36%` | `better` |
| `segment-index-mixed-drain:partitionedIngestMixed:getWorkload` | `241088.552 ops/s` | `296229.457 ops/s` | `+22.87%` | `better` |
| `segment-index-mixed-drain:partitionedIngestMixed:putWorkload` | `271732.540 ops/s` | `295379.828 ops/s` | `+8.70%` | `better` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed` | `899354.301 ops/s` | `1175456.833 ops/s` | `+30.70%` | `better` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:getWorkload` | `858591.152 ops/s` | `1137682.974 ops/s` | `+32.51%` | `better` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:putWorkload` | `40763.149 ops/s` | `37773.860 ops/s` | `-7.33%` | `worse` |
| `segment-index-persisted-mutation-concurrent:deleteSync` | `6347.061 ops/s` | `6876.680 ops/s` | `+8.34%` | `better` |
| `segment-index-persisted-mutation-concurrent:putSync` | `6058.905 ops/s` | `6866.753 ops/s` | `+13.33%` | `better` |
| `segment-index-persisted-mutation:deleteSync` | `2803.198 ops/s` | `3194.413 ops/s` | `+13.96%` | `better` |
| `segment-index-persisted-mutation:putSync` | `2751.106 ops/s` | `1828.968 ops/s` | `-33.52%` | `worse` |
| `segment-index-range-scan:boundedScan` | `21.413 us/op` | `23.700 us/op` | `+10.68%` | `better` |
| `segment-index-range-scan:fullStreamRangeFallback` | `1728.798 us/op` | `1567.666 us/op` | `-9.32%` | `worse` |
| `segment-index-range-scan:sequentialRead` | `3245.708 us/op` | `3137.352 us/op` | `-3.34%` | `warning` |
| `segment-merge-sequential:mergeSequential` | `274.445 us/op` | `279.105 us/op` | `+1.70%` | `neutral` |
