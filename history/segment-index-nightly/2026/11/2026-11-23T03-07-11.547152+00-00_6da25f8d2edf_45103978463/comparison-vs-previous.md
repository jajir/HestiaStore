# Benchmark Comparison

- Profile: `segment-index-nightly`
- Baseline SHA: `6da25f8d2edfcb35fc090cb0bcf177026066b7a3`
- Candidate SHA: `6da25f8d2edfcb35fc090cb0bcf177026066b7a3`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `segment-index-get-live:getHitSync` | `2818201.496 ops/s` | `6452552.904 ops/s` | `+128.96%` | `better` |
| `segment-index-get-live:getMissSync` | `2508747.104 ops/s` | `6204044.132 ops/s` | `+147.30%` | `better` |
| `segment-index-get-multisegment-cold:getHitSync` | `293646.267 ops/s` | `470707.901 ops/s` | `+60.30%` | `better` |
| `segment-index-get-multisegment-cold:getMissSync` | `2652049.086 ops/s` | `6449652.879 ops/s` | `+143.20%` | `better` |
| `segment-index-get-multisegment-hot:getHitSync` | `1631792.501 ops/s` | `5758253.604 ops/s` | `+252.88%` | `better` |
| `segment-index-get-multisegment-hot:getMissSync` | `2621302.522 ops/s` | `6546712.691 ops/s` | `+149.75%` | `better` |
| `segment-index-get-persisted:getHitSync` | `1918539.490 ops/s` | `4613941.089 ops/s` | `+140.49%` | `better` |
| `segment-index-get-persisted:getMissSync` | `2846990.546 ops/s` | `6513388.050 ops/s` | `+128.78%` | `better` |
| `segment-index-hot-route-put:putHotRoute` | `2324536.004 ops/s` | `5412341.654 ops/s` | `+132.84%` | `better` |
| `segment-index-hot-route-put:putThenGetHotRoute` | `1271114.732 ops/s` | `3239430.234 ops/s` | `+154.85%` | `better` |
| `segment-index-lifecycle:openAndCheckAndRepairConsistency` | `123.364 ms/op` | `201.513 ms/op` | `+63.35%` | `better` |
| `segment-index-lifecycle:openAndCompact` | `139.045 ms/op` | `213.773 ms/op` | `+53.74%` | `better` |
| `segment-index-lifecycle:openExisting` | `119.914 ms/op` | `202.133 ms/op` | `+68.57%` | `better` |
| `segment-index-mixed-drain:partitionedIngestMixed` | `475662.350 ops/s` | `707723.324 ops/s` | `+48.79%` | `better` |
| `segment-index-mixed-drain:partitionedIngestMixed:getWorkload` | `214565.873 ops/s` | `417893.555 ops/s` | `+94.76%` | `better` |
| `segment-index-mixed-drain:partitionedIngestMixed:putWorkload` | `261096.478 ops/s` | `289829.769 ops/s` | `+11.00%` | `better` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed` | `845524.105 ops/s` | `1778419.210 ops/s` | `+110.33%` | `better` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:getWorkload` | `804543.375 ops/s` | `1729985.154 ops/s` | `+115.03%` | `better` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:putWorkload` | `40980.730 ops/s` | `48434.056 ops/s` | `+18.19%` | `better` |
| `segment-index-persisted-mutation-concurrent:deleteSync` | `5863.162 ops/s` | `8327.596 ops/s` | `+42.03%` | `better` |
| `segment-index-persisted-mutation-concurrent:putSync` | `5839.257 ops/s` | `7878.455 ops/s` | `+34.92%` | `better` |
| `segment-index-persisted-mutation:deleteSync` | `2621.924 ops/s` | `3812.864 ops/s` | `+45.42%` | `better` |
| `segment-index-persisted-mutation:putSync` | `2601.806 ops/s` | `3733.570 ops/s` | `+43.50%` | `better` |
| `segment-index-range-scan:boundedScan` | `24.292 us/op` | `18.022 us/op` | `-25.81%` | `worse` |
| `segment-index-range-scan:fullStreamRangeFallback` | `1827.042 us/op` | `1171.262 us/op` | `-35.89%` | `worse` |
| `segment-index-range-scan:sequentialRead` | `3526.502 us/op` | `2317.465 us/op` | `-34.28%` | `worse` |
| `segment-merge-sequential:mergeSequential` | `311.295 us/op` | `182.901 us/op` | `-41.25%` | `worse` |
