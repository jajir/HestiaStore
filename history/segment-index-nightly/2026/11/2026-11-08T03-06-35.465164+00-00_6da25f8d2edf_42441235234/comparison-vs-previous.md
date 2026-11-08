# Benchmark Comparison

- Profile: `segment-index-nightly`
- Baseline SHA: `6da25f8d2edfcb35fc090cb0bcf177026066b7a3`
- Candidate SHA: `6da25f8d2edfcb35fc090cb0bcf177026066b7a3`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `segment-index-get-live:getHitSync` | `4847025.080 ops/s` | `2677821.425 ops/s` | `-44.75%` | `worse` |
| `segment-index-get-live:getMissSync` | `3941599.347 ops/s` | `2797094.445 ops/s` | `-29.04%` | `worse` |
| `segment-index-get-multisegment-cold:getHitSync` | `290134.092 ops/s` | `301769.930 ops/s` | `+4.01%` | `better` |
| `segment-index-get-multisegment-cold:getMissSync` | `4390162.768 ops/s` | `2863895.987 ops/s` | `-34.77%` | `worse` |
| `segment-index-get-multisegment-hot:getHitSync` | `3247766.866 ops/s` | `2180148.895 ops/s` | `-32.87%` | `worse` |
| `segment-index-get-multisegment-hot:getMissSync` | `4631304.020 ops/s` | `2682361.843 ops/s` | `-42.08%` | `worse` |
| `segment-index-get-persisted:getHitSync` | `3299638.486 ops/s` | `1785842.705 ops/s` | `-45.88%` | `worse` |
| `segment-index-get-persisted:getMissSync` | `4471064.413 ops/s` | `2684080.317 ops/s` | `-39.97%` | `worse` |
| `segment-index-hot-route-put:putHotRoute` | `3766008.623 ops/s` | `2410641.004 ops/s` | `-35.99%` | `worse` |
| `segment-index-hot-route-put:putThenGetHotRoute` | `2173201.877 ops/s` | `1316215.883 ops/s` | `-39.43%` | `worse` |
| `segment-index-lifecycle:openAndCheckAndRepairConsistency` | `225.289 ms/op` | `119.720 ms/op` | `-46.86%` | `worse` |
| `segment-index-lifecycle:openAndCompact` | `240.122 ms/op` | `136.244 ms/op` | `-43.26%` | `worse` |
| `segment-index-lifecycle:openExisting` | `220.045 ms/op` | `117.979 ms/op` | `-46.38%` | `worse` |
| `segment-index-mixed-drain:partitionedIngestMixed` | `545591.009 ops/s` | `483780.408 ops/s` | `-11.33%` | `worse` |
| `segment-index-mixed-drain:partitionedIngestMixed:getWorkload` | `256113.348 ops/s` | `222470.810 ops/s` | `-13.14%` | `worse` |
| `segment-index-mixed-drain:partitionedIngestMixed:putWorkload` | `289477.661 ops/s` | `261309.598 ops/s` | `-9.73%` | `worse` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed` | `1134407.799 ops/s` | `871013.921 ops/s` | `-23.22%` | `worse` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:getWorkload` | `1099121.819 ops/s` | `830903.492 ops/s` | `-24.40%` | `worse` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:putWorkload` | `35285.980 ops/s` | `40110.429 ops/s` | `+13.67%` | `better` |
| `segment-index-persisted-mutation-concurrent:deleteSync` | `7801.964 ops/s` | `6123.823 ops/s` | `-21.51%` | `worse` |
| `segment-index-persisted-mutation-concurrent:putSync` | `7805.379 ops/s` | `6186.730 ops/s` | `-20.74%` | `worse` |
| `segment-index-persisted-mutation:deleteSync` | `3574.286 ops/s` | `2749.806 ops/s` | `-23.07%` | `worse` |
| `segment-index-persisted-mutation:putSync` | `3434.566 ops/s` | `2734.616 ops/s` | `-20.38%` | `worse` |
| `segment-index-range-scan:boundedScan` | `27.250 us/op` | `22.395 us/op` | `-17.82%` | `worse` |
| `segment-index-range-scan:fullStreamRangeFallback` | `2027.723 us/op` | `1831.456 us/op` | `-9.68%` | `worse` |
| `segment-index-range-scan:sequentialRead` | `3832.334 us/op` | `3577.061 us/op` | `-6.66%` | `warning` |
| `segment-merge-sequential:mergeSequential` | `336.393 us/op` | `313.651 us/op` | `-6.76%` | `warning` |
