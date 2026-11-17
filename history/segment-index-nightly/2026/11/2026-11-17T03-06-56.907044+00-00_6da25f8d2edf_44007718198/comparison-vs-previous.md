# Benchmark Comparison

- Profile: `segment-index-nightly`
- Baseline SHA: `6da25f8d2edfcb35fc090cb0bcf177026066b7a3`
- Candidate SHA: `6da25f8d2edfcb35fc090cb0bcf177026066b7a3`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `segment-index-get-live:getHitSync` | `3309258.391 ops/s` | `3250972.470 ops/s` | `-1.76%` | `neutral` |
| `segment-index-get-live:getMissSync` | `2949726.803 ops/s` | `2820569.251 ops/s` | `-4.38%` | `warning` |
| `segment-index-get-multisegment-cold:getHitSync` | `378156.075 ops/s` | `375911.106 ops/s` | `-0.59%` | `neutral` |
| `segment-index-get-multisegment-cold:getMissSync` | `2714013.318 ops/s` | `2602459.469 ops/s` | `-4.11%` | `warning` |
| `segment-index-get-multisegment-hot:getHitSync` | `2256612.601 ops/s` | `2080499.046 ops/s` | `-7.80%` | `worse` |
| `segment-index-get-multisegment-hot:getMissSync` | `2713895.884 ops/s` | `2657228.597 ops/s` | `-2.09%` | `neutral` |
| `segment-index-get-persisted:getHitSync` | `2265651.939 ops/s` | `2288324.821 ops/s` | `+1.00%` | `neutral` |
| `segment-index-get-persisted:getMissSync` | `2460916.892 ops/s` | `2415604.731 ops/s` | `-1.84%` | `neutral` |
| `segment-index-hot-route-put:putHotRoute` | `2567626.733 ops/s` | `2691864.273 ops/s` | `+4.84%` | `better` |
| `segment-index-hot-route-put:putThenGetHotRoute` | `1319711.584 ops/s` | `1391944.423 ops/s` | `+5.47%` | `better` |
| `segment-index-lifecycle:openAndCheckAndRepairConsistency` | `86.275 ms/op` | `86.542 ms/op` | `+0.31%` | `neutral` |
| `segment-index-lifecycle:openAndCompact` | `96.777 ms/op` | `99.637 ms/op` | `+2.95%` | `neutral` |
| `segment-index-lifecycle:openExisting` | `83.252 ms/op` | `84.975 ms/op` | `+2.07%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed` | `594424.981 ops/s` | `580888.001 ops/s` | `-2.28%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed:getWorkload` | `304736.862 ops/s` | `292489.407 ops/s` | `-4.02%` | `warning` |
| `segment-index-mixed-drain:partitionedIngestMixed:putWorkload` | `289688.119 ops/s` | `288398.594 ops/s` | `-0.45%` | `neutral` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed` | `1004920.817 ops/s` | `989420.379 ops/s` | `-1.54%` | `neutral` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:getWorkload` | `957118.870 ops/s` | `941319.999 ops/s` | `-1.65%` | `neutral` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:putWorkload` | `47801.947 ops/s` | `48100.380 ops/s` | `+0.62%` | `neutral` |
| `segment-index-persisted-mutation-concurrent:deleteSync` | `12870.475 ops/s` | `12309.437 ops/s` | `-4.36%` | `warning` |
| `segment-index-persisted-mutation-concurrent:putSync` | `14467.019 ops/s` | `14806.661 ops/s` | `+2.35%` | `neutral` |
| `segment-index-persisted-mutation:deleteSync` | `6569.827 ops/s` | `6468.711 ops/s` | `-1.54%` | `neutral` |
| `segment-index-persisted-mutation:putSync` | `6006.475 ops/s` | `5619.798 ops/s` | `-6.44%` | `warning` |
| `segment-index-range-scan:boundedScan` | `15.998 us/op` | `19.167 us/op` | `+19.81%` | `better` |
| `segment-index-range-scan:fullStreamRangeFallback` | `1348.705 us/op` | `1362.262 us/op` | `+1.01%` | `neutral` |
| `segment-index-range-scan:sequentialRead` | `2607.498 us/op` | `2626.480 us/op` | `+0.73%` | `neutral` |
| `segment-merge-sequential:mergeSequential` | `243.895 us/op` | `244.269 us/op` | `+0.15%` | `neutral` |
