# Benchmark Comparison

- Profile: `segment-index-nightly`
- Baseline SHA: `6da25f8d2edfcb35fc090cb0bcf177026066b7a3`
- Candidate SHA: `6da25f8d2edfcb35fc090cb0bcf177026066b7a3`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `segment-index-get-live:getHitSync` | `3281922.532 ops/s` | `3309258.391 ops/s` | `+0.83%` | `neutral` |
| `segment-index-get-live:getMissSync` | `2072141.072 ops/s` | `2949726.803 ops/s` | `+42.35%` | `better` |
| `segment-index-get-multisegment-cold:getHitSync` | `387665.523 ops/s` | `378156.075 ops/s` | `-2.45%` | `neutral` |
| `segment-index-get-multisegment-cold:getMissSync` | `2812386.509 ops/s` | `2714013.318 ops/s` | `-3.50%` | `warning` |
| `segment-index-get-multisegment-hot:getHitSync` | `2088839.303 ops/s` | `2256612.601 ops/s` | `+8.03%` | `better` |
| `segment-index-get-multisegment-hot:getMissSync` | `3101235.188 ops/s` | `2713895.884 ops/s` | `-12.49%` | `worse` |
| `segment-index-get-persisted:getHitSync` | `2144268.125 ops/s` | `2265651.939 ops/s` | `+5.66%` | `better` |
| `segment-index-get-persisted:getMissSync` | `2369644.712 ops/s` | `2460916.892 ops/s` | `+3.85%` | `better` |
| `segment-index-hot-route-put:putHotRoute` | `2785295.846 ops/s` | `2567626.733 ops/s` | `-7.81%` | `worse` |
| `segment-index-hot-route-put:putThenGetHotRoute` | `1466321.661 ops/s` | `1319711.584 ops/s` | `-10.00%` | `worse` |
| `segment-index-lifecycle:openAndCheckAndRepairConsistency` | `84.797 ms/op` | `86.275 ms/op` | `+1.74%` | `neutral` |
| `segment-index-lifecycle:openAndCompact` | `96.955 ms/op` | `96.777 ms/op` | `-0.18%` | `neutral` |
| `segment-index-lifecycle:openExisting` | `81.914 ms/op` | `83.252 ms/op` | `+1.63%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed` | `597255.589 ops/s` | `594424.981 ops/s` | `-0.47%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed:getWorkload` | `308554.250 ops/s` | `304736.862 ops/s` | `-1.24%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed:putWorkload` | `288701.339 ops/s` | `289688.119 ops/s` | `+0.34%` | `neutral` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed` | `983944.350 ops/s` | `1004920.817 ops/s` | `+2.13%` | `neutral` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:getWorkload` | `935145.945 ops/s` | `957118.870 ops/s` | `+2.35%` | `neutral` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:putWorkload` | `48798.405 ops/s` | `47801.947 ops/s` | `-2.04%` | `neutral` |
| `segment-index-persisted-mutation-concurrent:deleteSync` | `12997.046 ops/s` | `12870.475 ops/s` | `-0.97%` | `neutral` |
| `segment-index-persisted-mutation-concurrent:putSync` | `14068.248 ops/s` | `14467.019 ops/s` | `+2.83%` | `neutral` |
| `segment-index-persisted-mutation:deleteSync` | `6385.362 ops/s` | `6569.827 ops/s` | `+2.89%` | `neutral` |
| `segment-index-persisted-mutation:putSync` | `6110.278 ops/s` | `6006.475 ops/s` | `-1.70%` | `neutral` |
| `segment-index-range-scan:boundedScan` | `18.035 us/op` | `15.998 us/op` | `-11.29%` | `worse` |
| `segment-index-range-scan:fullStreamRangeFallback` | `1327.674 us/op` | `1348.705 us/op` | `+1.58%` | `neutral` |
| `segment-index-range-scan:sequentialRead` | `2589.801 us/op` | `2607.498 us/op` | `+0.68%` | `neutral` |
| `segment-merge-sequential:mergeSequential` | `239.081 us/op` | `243.895 us/op` | `+2.01%` | `neutral` |
