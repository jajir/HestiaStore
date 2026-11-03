# Benchmark Comparison

- Profile: `segment-index-nightly`
- Baseline SHA: `6da25f8d2edfcb35fc090cb0bcf177026066b7a3`
- Candidate SHA: `6da25f8d2edfcb35fc090cb0bcf177026066b7a3`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `segment-index-get-live:getHitSync` | `4930142.636 ops/s` | `2930674.171 ops/s` | `-40.56%` | `worse` |
| `segment-index-get-live:getMissSync` | `4685283.775 ops/s` | `2950328.807 ops/s` | `-37.03%` | `worse` |
| `segment-index-get-multisegment-cold:getHitSync` | `353852.420 ops/s` | `303431.750 ops/s` | `-14.25%` | `worse` |
| `segment-index-get-multisegment-cold:getMissSync` | `5465775.400 ops/s` | `3334200.101 ops/s` | `-39.00%` | `worse` |
| `segment-index-get-multisegment-hot:getHitSync` | `4225724.639 ops/s` | `1789016.166 ops/s` | `-57.66%` | `worse` |
| `segment-index-get-multisegment-hot:getMissSync` | `5402114.525 ops/s` | `2832598.311 ops/s` | `-47.57%` | `worse` |
| `segment-index-get-persisted:getHitSync` | `3322968.490 ops/s` | `1888652.021 ops/s` | `-43.16%` | `worse` |
| `segment-index-get-persisted:getMissSync` | `4749856.951 ops/s` | `2323798.264 ops/s` | `-51.08%` | `worse` |
| `segment-index-hot-route-put:putHotRoute` | `4410588.353 ops/s` | `2799197.783 ops/s` | `-36.53%` | `worse` |
| `segment-index-hot-route-put:putThenGetHotRoute` | `2397532.993 ops/s` | `1305031.693 ops/s` | `-45.57%` | `worse` |
| `segment-index-lifecycle:openAndCheckAndRepairConsistency` | `239.275 ms/op` | `121.551 ms/op` | `-49.20%` | `worse` |
| `segment-index-lifecycle:openAndCompact` | `258.856 ms/op` | `137.245 ms/op` | `-46.98%` | `worse` |
| `segment-index-lifecycle:openExisting` | `243.475 ms/op` | `119.544 ms/op` | `-50.90%` | `worse` |
| `segment-index-mixed-drain:partitionedIngestMixed` | `591609.286 ops/s` | `504791.554 ops/s` | `-14.67%` | `worse` |
| `segment-index-mixed-drain:partitionedIngestMixed:getWorkload` | `296229.457 ops/s` | `238405.273 ops/s` | `-19.52%` | `worse` |
| `segment-index-mixed-drain:partitionedIngestMixed:putWorkload` | `295379.828 ops/s` | `266386.281 ops/s` | `-9.82%` | `worse` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed` | `1175456.833 ops/s` | `938953.736 ops/s` | `-20.12%` | `worse` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:getWorkload` | `1137682.974 ops/s` | `900101.364 ops/s` | `-20.88%` | `worse` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:putWorkload` | `37773.860 ops/s` | `38852.372 ops/s` | `+2.86%` | `neutral` |
| `segment-index-persisted-mutation-concurrent:deleteSync` | `6876.680 ops/s` | `5567.861 ops/s` | `-19.03%` | `worse` |
| `segment-index-persisted-mutation-concurrent:putSync` | `6866.753 ops/s` | `5846.319 ops/s` | `-14.86%` | `worse` |
| `segment-index-persisted-mutation:deleteSync` | `3194.413 ops/s` | `2547.728 ops/s` | `-20.24%` | `worse` |
| `segment-index-persisted-mutation:putSync` | `1828.968 ops/s` | `2523.527 ops/s` | `+37.98%` | `better` |
| `segment-index-range-scan:boundedScan` | `23.700 us/op` | `22.550 us/op` | `-4.85%` | `warning` |
| `segment-index-range-scan:fullStreamRangeFallback` | `1567.666 us/op` | `1837.318 us/op` | `+17.20%` | `better` |
| `segment-index-range-scan:sequentialRead` | `3137.352 us/op` | `3508.877 us/op` | `+11.84%` | `better` |
| `segment-merge-sequential:mergeSequential` | `279.105 us/op` | `308.378 us/op` | `+10.49%` | `better` |
