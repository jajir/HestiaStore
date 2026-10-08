# Benchmark Comparison

- Profile: `segment-index-nightly`
- Baseline SHA: `6da25f8d2edfcb35fc090cb0bcf177026066b7a3`
- Candidate SHA: `6da25f8d2edfcb35fc090cb0bcf177026066b7a3`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `segment-index-get-live:getHitSync` | `4546717.424 ops/s` | `2833949.586 ops/s` | `-37.67%` | `worse` |
| `segment-index-get-live:getMissSync` | `4632176.167 ops/s` | `2443716.098 ops/s` | `-47.24%` | `worse` |
| `segment-index-get-multisegment-cold:getHitSync` | `264398.072 ops/s` | `301831.893 ops/s` | `+14.16%` | `better` |
| `segment-index-get-multisegment-cold:getMissSync` | `4874860.701 ops/s` | `2310733.336 ops/s` | `-52.60%` | `worse` |
| `segment-index-get-multisegment-hot:getHitSync` | `3803765.044 ops/s` | `1868020.731 ops/s` | `-50.89%` | `worse` |
| `segment-index-get-multisegment-hot:getMissSync` | `4569329.101 ops/s` | `2599744.538 ops/s` | `-43.10%` | `worse` |
| `segment-index-get-persisted:getHitSync` | `3397455.713 ops/s` | `1920098.164 ops/s` | `-43.48%` | `worse` |
| `segment-index-get-persisted:getMissSync` | `4765402.559 ops/s` | `2688058.649 ops/s` | `-43.59%` | `worse` |
| `segment-index-hot-route-put:putHotRoute` | `4365954.501 ops/s` | `2455823.370 ops/s` | `-43.75%` | `worse` |
| `segment-index-hot-route-put:putThenGetHotRoute` | `2277296.722 ops/s` | `1549878.566 ops/s` | `-31.94%` | `worse` |
| `segment-index-lifecycle:openAndCheckAndRepairConsistency` | `247.548 ms/op` | `136.669 ms/op` | `-44.79%` | `worse` |
| `segment-index-lifecycle:openAndCompact` | `261.546 ms/op` | `151.755 ms/op` | `-41.98%` | `worse` |
| `segment-index-lifecycle:openExisting` | `241.535 ms/op` | `133.767 ms/op` | `-44.62%` | `worse` |
| `segment-index-mixed-drain:partitionedIngestMixed` | `554887.420 ops/s` | `486431.313 ops/s` | `-12.34%` | `worse` |
| `segment-index-mixed-drain:partitionedIngestMixed:getWorkload` | `279760.553 ops/s` | `219323.731 ops/s` | `-21.60%` | `worse` |
| `segment-index-mixed-drain:partitionedIngestMixed:putWorkload` | `275126.867 ops/s` | `267107.582 ops/s` | `-2.91%` | `neutral` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed` | `1308839.318 ops/s` | `862452.540 ops/s` | `-34.11%` | `worse` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:getWorkload` | `1273034.344 ops/s` | `822557.073 ops/s` | `-35.39%` | `worse` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:putWorkload` | `35804.974 ops/s` | `39895.468 ops/s` | `+11.42%` | `better` |
| `segment-index-persisted-mutation-concurrent:deleteSync` | `8639.310 ops/s` | `6082.577 ops/s` | `-29.59%` | `worse` |
| `segment-index-persisted-mutation-concurrent:putSync` | `8482.244 ops/s` | `6392.007 ops/s` | `-24.64%` | `worse` |
| `segment-index-persisted-mutation:deleteSync` | `3191.425 ops/s` | `2791.004 ops/s` | `-12.55%` | `worse` |
| `segment-index-persisted-mutation:putSync` | `3070.342 ops/s` | `2715.919 ops/s` | `-11.54%` | `worse` |
| `segment-index-range-scan:boundedScan` | `30.541 us/op` | `24.507 us/op` | `-19.76%` | `worse` |
| `segment-index-range-scan:fullStreamRangeFallback` | `2021.308 us/op` | `1882.920 us/op` | `-6.85%` | `warning` |
| `segment-index-range-scan:sequentialRead` | `3801.762 us/op` | `3593.667 us/op` | `-5.47%` | `warning` |
| `segment-merge-sequential:mergeSequential` | `332.546 us/op` | `313.437 us/op` | `-5.75%` | `warning` |
