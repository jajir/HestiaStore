# Benchmark Comparison

- Profile: `segment-index-nightly`
- Baseline SHA: `6da25f8d2edfcb35fc090cb0bcf177026066b7a3`
- Candidate SHA: `6da25f8d2edfcb35fc090cb0bcf177026066b7a3`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `segment-index-get-live:getHitSync` | `2786159.666 ops/s` | `6167010.924 ops/s` | `+121.34%` | `better` |
| `segment-index-get-live:getMissSync` | `3086754.571 ops/s` | `6713107.739 ops/s` | `+117.48%` | `better` |
| `segment-index-get-multisegment-cold:getHitSync` | `398667.520 ops/s` | `474162.075 ops/s` | `+18.94%` | `better` |
| `segment-index-get-multisegment-cold:getMissSync` | `2811664.257 ops/s` | `6589640.233 ops/s` | `+134.37%` | `better` |
| `segment-index-get-multisegment-hot:getHitSync` | `2191388.922 ops/s` | `5144668.972 ops/s` | `+134.77%` | `better` |
| `segment-index-get-multisegment-hot:getMissSync` | `2852724.962 ops/s` | `6192154.187 ops/s` | `+117.06%` | `better` |
| `segment-index-get-persisted:getHitSync` | `2384026.538 ops/s` | `5321859.883 ops/s` | `+123.23%` | `better` |
| `segment-index-get-persisted:getMissSync` | `2857833.249 ops/s` | `6616327.033 ops/s` | `+131.52%` | `better` |
| `segment-index-hot-route-put:putHotRoute` | `2552529.521 ops/s` | `6070464.367 ops/s` | `+137.82%` | `better` |
| `segment-index-hot-route-put:putThenGetHotRoute` | `1336464.218 ops/s` | `3057247.958 ops/s` | `+128.76%` | `better` |
| `segment-index-lifecycle:openAndCheckAndRepairConsistency` | `85.017 ms/op` | `201.698 ms/op` | `+137.25%` | `better` |
| `segment-index-lifecycle:openAndCompact` | `95.570 ms/op` | `221.743 ms/op` | `+132.02%` | `better` |
| `segment-index-lifecycle:openExisting` | `83.286 ms/op` | `203.795 ms/op` | `+144.69%` | `better` |
| `segment-index-mixed-drain:partitionedIngestMixed` | `600334.315 ops/s` | `678769.801 ops/s` | `+13.07%` | `better` |
| `segment-index-mixed-drain:partitionedIngestMixed:getWorkload` | `315685.658 ops/s` | `394317.990 ops/s` | `+24.91%` | `better` |
| `segment-index-mixed-drain:partitionedIngestMixed:putWorkload` | `284648.658 ops/s` | `284451.811 ops/s` | `-0.07%` | `neutral` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed` | `1004824.195 ops/s` | `1993251.996 ops/s` | `+98.37%` | `better` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:getWorkload` | `955391.896 ops/s` | `1945027.260 ops/s` | `+103.58%` | `better` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:putWorkload` | `49432.299 ops/s` | `48224.736 ops/s` | `-2.44%` | `neutral` |
| `segment-index-persisted-mutation-concurrent:deleteSync` | `13285.667 ops/s` | `6175.005 ops/s` | `-53.52%` | `worse` |
| `segment-index-persisted-mutation-concurrent:putSync` | `14627.861 ops/s` | `7051.986 ops/s` | `-51.79%` | `worse` |
| `segment-index-persisted-mutation:deleteSync` | `6490.609 ops/s` | `2789.507 ops/s` | `-57.02%` | `worse` |
| `segment-index-persisted-mutation:putSync` | `6206.032 ops/s` | `3263.992 ops/s` | `-47.41%` | `worse` |
| `segment-index-range-scan:boundedScan` | `16.875 us/op` | `19.268 us/op` | `+14.18%` | `better` |
| `segment-index-range-scan:fullStreamRangeFallback` | `1303.465 us/op` | `1281.915 us/op` | `-1.65%` | `neutral` |
| `segment-index-range-scan:sequentialRead` | `2540.564 us/op` | `2374.523 us/op` | `-6.54%` | `warning` |
| `segment-merge-sequential:mergeSequential` | `248.275 us/op` | `182.877 us/op` | `-26.34%` | `worse` |
