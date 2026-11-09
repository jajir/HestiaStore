# Benchmark Comparison

- Profile: `segment-index-nightly`
- Baseline SHA: `6da25f8d2edfcb35fc090cb0bcf177026066b7a3`
- Candidate SHA: `6da25f8d2edfcb35fc090cb0bcf177026066b7a3`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `segment-index-get-live:getHitSync` | `2677821.425 ops/s` | `4800335.211 ops/s` | `+79.26%` | `better` |
| `segment-index-get-live:getMissSync` | `2797094.445 ops/s` | `4554064.138 ops/s` | `+62.81%` | `better` |
| `segment-index-get-multisegment-cold:getHitSync` | `301769.930 ops/s` | `270937.824 ops/s` | `-10.22%` | `worse` |
| `segment-index-get-multisegment-cold:getMissSync` | `2863895.987 ops/s` | `4205274.778 ops/s` | `+46.84%` | `better` |
| `segment-index-get-multisegment-hot:getHitSync` | `2180148.895 ops/s` | `3309106.774 ops/s` | `+51.78%` | `better` |
| `segment-index-get-multisegment-hot:getMissSync` | `2682361.843 ops/s` | `3378470.212 ops/s` | `+25.95%` | `better` |
| `segment-index-get-persisted:getHitSync` | `1785842.705 ops/s` | `3072767.995 ops/s` | `+72.06%` | `better` |
| `segment-index-get-persisted:getMissSync` | `2684080.317 ops/s` | `4206836.644 ops/s` | `+56.73%` | `better` |
| `segment-index-hot-route-put:putHotRoute` | `2410641.004 ops/s` | `4052317.510 ops/s` | `+68.10%` | `better` |
| `segment-index-hot-route-put:putThenGetHotRoute` | `1316215.883 ops/s` | `2074773.792 ops/s` | `+57.63%` | `better` |
| `segment-index-lifecycle:openAndCheckAndRepairConsistency` | `119.720 ms/op` | `227.994 ms/op` | `+90.44%` | `better` |
| `segment-index-lifecycle:openAndCompact` | `136.244 ms/op` | `242.377 ms/op` | `+77.90%` | `better` |
| `segment-index-lifecycle:openExisting` | `117.979 ms/op` | `225.556 ms/op` | `+91.18%` | `better` |
| `segment-index-mixed-drain:partitionedIngestMixed` | `483780.408 ops/s` | `497415.430 ops/s` | `+2.82%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed:getWorkload` | `222470.810 ops/s` | `248665.790 ops/s` | `+11.77%` | `better` |
| `segment-index-mixed-drain:partitionedIngestMixed:putWorkload` | `261309.598 ops/s` | `248749.640 ops/s` | `-4.81%` | `warning` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed` | `871013.921 ops/s` | `1110352.803 ops/s` | `+27.48%` | `better` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:getWorkload` | `830903.492 ops/s` | `1079095.857 ops/s` | `+29.87%` | `better` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:putWorkload` | `40110.429 ops/s` | `31256.945 ops/s` | `-22.07%` | `worse` |
| `segment-index-persisted-mutation-concurrent:deleteSync` | `6123.823 ops/s` | `6204.939 ops/s` | `+1.32%` | `neutral` |
| `segment-index-persisted-mutation-concurrent:putSync` | `6186.730 ops/s` | `6053.535 ops/s` | `-2.15%` | `neutral` |
| `segment-index-persisted-mutation:deleteSync` | `2749.806 ops/s` | `2794.692 ops/s` | `+1.63%` | `neutral` |
| `segment-index-persisted-mutation:putSync` | `2734.616 ops/s` | `2672.083 ops/s` | `-2.29%` | `neutral` |
| `segment-index-range-scan:boundedScan` | `22.395 us/op` | `29.558 us/op` | `+31.99%` | `better` |
| `segment-index-range-scan:fullStreamRangeFallback` | `1831.456 us/op` | `2031.744 us/op` | `+10.94%` | `better` |
| `segment-index-range-scan:sequentialRead` | `3577.061 us/op` | `3997.761 us/op` | `+11.76%` | `better` |
| `segment-merge-sequential:mergeSequential` | `313.651 us/op` | `334.970 us/op` | `+6.80%` | `better` |
