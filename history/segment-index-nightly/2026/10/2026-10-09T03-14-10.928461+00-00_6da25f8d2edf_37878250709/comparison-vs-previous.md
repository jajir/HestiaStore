# Benchmark Comparison

- Profile: `segment-index-nightly`
- Baseline SHA: `6da25f8d2edfcb35fc090cb0bcf177026066b7a3`
- Candidate SHA: `6da25f8d2edfcb35fc090cb0bcf177026066b7a3`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `segment-index-get-live:getHitSync` | `2833949.586 ops/s` | `5124424.918 ops/s` | `+80.82%` | `better` |
| `segment-index-get-live:getMissSync` | `2443716.098 ops/s` | `4648467.440 ops/s` | `+90.22%` | `better` |
| `segment-index-get-multisegment-cold:getHitSync` | `301831.893 ops/s` | `271955.503 ops/s` | `-9.90%` | `worse` |
| `segment-index-get-multisegment-cold:getMissSync` | `2310733.336 ops/s` | `4477732.817 ops/s` | `+93.78%` | `better` |
| `segment-index-get-multisegment-hot:getHitSync` | `1868020.731 ops/s` | `3683703.051 ops/s` | `+97.20%` | `better` |
| `segment-index-get-multisegment-hot:getMissSync` | `2599744.538 ops/s` | `4323613.182 ops/s` | `+66.31%` | `better` |
| `segment-index-get-persisted:getHitSync` | `1920098.164 ops/s` | `3404859.188 ops/s` | `+77.33%` | `better` |
| `segment-index-get-persisted:getMissSync` | `2688058.649 ops/s` | `4475267.112 ops/s` | `+66.49%` | `better` |
| `segment-index-hot-route-put:putHotRoute` | `2455823.370 ops/s` | `4132125.596 ops/s` | `+68.26%` | `better` |
| `segment-index-hot-route-put:putThenGetHotRoute` | `1549878.566 ops/s` | `2364291.353 ops/s` | `+52.55%` | `better` |
| `segment-index-lifecycle:openAndCheckAndRepairConsistency` | `136.669 ms/op` | `244.163 ms/op` | `+78.65%` | `better` |
| `segment-index-lifecycle:openAndCompact` | `151.755 ms/op` | `260.686 ms/op` | `+71.78%` | `better` |
| `segment-index-lifecycle:openExisting` | `133.767 ms/op` | `242.674 ms/op` | `+81.41%` | `better` |
| `segment-index-mixed-drain:partitionedIngestMixed` | `486431.313 ops/s` | `556237.684 ops/s` | `+14.35%` | `better` |
| `segment-index-mixed-drain:partitionedIngestMixed:getWorkload` | `219323.731 ops/s` | `263337.398 ops/s` | `+20.07%` | `better` |
| `segment-index-mixed-drain:partitionedIngestMixed:putWorkload` | `267107.582 ops/s` | `292900.286 ops/s` | `+9.66%` | `better` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed` | `862452.540 ops/s` | `1182753.807 ops/s` | `+37.14%` | `better` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:getWorkload` | `822557.073 ops/s` | `1146620.218 ops/s` | `+39.40%` | `better` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:putWorkload` | `39895.468 ops/s` | `36133.588 ops/s` | `-9.43%` | `worse` |
| `segment-index-persisted-mutation-concurrent:deleteSync` | `6082.577 ops/s` | `8490.515 ops/s` | `+39.59%` | `better` |
| `segment-index-persisted-mutation-concurrent:putSync` | `6392.007 ops/s` | `8572.081 ops/s` | `+34.11%` | `better` |
| `segment-index-persisted-mutation:deleteSync` | `2791.004 ops/s` | `3193.775 ops/s` | `+14.43%` | `better` |
| `segment-index-persisted-mutation:putSync` | `2715.919 ops/s` | `3064.465 ops/s` | `+12.83%` | `better` |
| `segment-index-range-scan:boundedScan` | `24.507 us/op` | `26.831 us/op` | `+9.48%` | `better` |
| `segment-index-range-scan:fullStreamRangeFallback` | `1882.920 us/op` | `2082.287 us/op` | `+10.59%` | `better` |
| `segment-index-range-scan:sequentialRead` | `3593.667 us/op` | `3921.514 us/op` | `+9.12%` | `better` |
| `segment-merge-sequential:mergeSequential` | `313.437 us/op` | `334.034 us/op` | `+6.57%` | `better` |
