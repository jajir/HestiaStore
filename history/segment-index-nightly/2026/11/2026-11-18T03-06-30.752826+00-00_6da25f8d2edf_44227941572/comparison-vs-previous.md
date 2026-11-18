# Benchmark Comparison

- Profile: `segment-index-nightly`
- Baseline SHA: `6da25f8d2edfcb35fc090cb0bcf177026066b7a3`
- Candidate SHA: `6da25f8d2edfcb35fc090cb0bcf177026066b7a3`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `segment-index-get-live:getHitSync` | `3250972.470 ops/s` | `2786159.666 ops/s` | `-14.30%` | `worse` |
| `segment-index-get-live:getMissSync` | `2820569.251 ops/s` | `3086754.571 ops/s` | `+9.44%` | `better` |
| `segment-index-get-multisegment-cold:getHitSync` | `375911.106 ops/s` | `398667.520 ops/s` | `+6.05%` | `better` |
| `segment-index-get-multisegment-cold:getMissSync` | `2602459.469 ops/s` | `2811664.257 ops/s` | `+8.04%` | `better` |
| `segment-index-get-multisegment-hot:getHitSync` | `2080499.046 ops/s` | `2191388.922 ops/s` | `+5.33%` | `better` |
| `segment-index-get-multisegment-hot:getMissSync` | `2657228.597 ops/s` | `2852724.962 ops/s` | `+7.36%` | `better` |
| `segment-index-get-persisted:getHitSync` | `2288324.821 ops/s` | `2384026.538 ops/s` | `+4.18%` | `better` |
| `segment-index-get-persisted:getMissSync` | `2415604.731 ops/s` | `2857833.249 ops/s` | `+18.31%` | `better` |
| `segment-index-hot-route-put:putHotRoute` | `2691864.273 ops/s` | `2552529.521 ops/s` | `-5.18%` | `warning` |
| `segment-index-hot-route-put:putThenGetHotRoute` | `1391944.423 ops/s` | `1336464.218 ops/s` | `-3.99%` | `warning` |
| `segment-index-lifecycle:openAndCheckAndRepairConsistency` | `86.542 ms/op` | `85.017 ms/op` | `-1.76%` | `neutral` |
| `segment-index-lifecycle:openAndCompact` | `99.637 ms/op` | `95.570 ms/op` | `-4.08%` | `warning` |
| `segment-index-lifecycle:openExisting` | `84.975 ms/op` | `83.286 ms/op` | `-1.99%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed` | `580888.001 ops/s` | `600334.315 ops/s` | `+3.35%` | `better` |
| `segment-index-mixed-drain:partitionedIngestMixed:getWorkload` | `292489.407 ops/s` | `315685.658 ops/s` | `+7.93%` | `better` |
| `segment-index-mixed-drain:partitionedIngestMixed:putWorkload` | `288398.594 ops/s` | `284648.658 ops/s` | `-1.30%` | `neutral` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed` | `989420.379 ops/s` | `1004824.195 ops/s` | `+1.56%` | `neutral` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:getWorkload` | `941319.999 ops/s` | `955391.896 ops/s` | `+1.49%` | `neutral` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:putWorkload` | `48100.380 ops/s` | `49432.299 ops/s` | `+2.77%` | `neutral` |
| `segment-index-persisted-mutation-concurrent:deleteSync` | `12309.437 ops/s` | `13285.667 ops/s` | `+7.93%` | `better` |
| `segment-index-persisted-mutation-concurrent:putSync` | `14806.661 ops/s` | `14627.861 ops/s` | `-1.21%` | `neutral` |
| `segment-index-persisted-mutation:deleteSync` | `6468.711 ops/s` | `6490.609 ops/s` | `+0.34%` | `neutral` |
| `segment-index-persisted-mutation:putSync` | `5619.798 ops/s` | `6206.032 ops/s` | `+10.43%` | `better` |
| `segment-index-range-scan:boundedScan` | `19.167 us/op` | `16.875 us/op` | `-11.96%` | `worse` |
| `segment-index-range-scan:fullStreamRangeFallback` | `1362.262 us/op` | `1303.465 us/op` | `-4.32%` | `warning` |
| `segment-index-range-scan:sequentialRead` | `2626.480 us/op` | `2540.564 us/op` | `-3.27%` | `warning` |
| `segment-merge-sequential:mergeSequential` | `244.269 us/op` | `248.275 us/op` | `+1.64%` | `neutral` |
