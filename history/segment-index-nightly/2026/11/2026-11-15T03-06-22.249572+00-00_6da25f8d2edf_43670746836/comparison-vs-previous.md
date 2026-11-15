# Benchmark Comparison

- Profile: `segment-index-nightly`
- Baseline SHA: `6da25f8d2edfcb35fc090cb0bcf177026066b7a3`
- Candidate SHA: `6da25f8d2edfcb35fc090cb0bcf177026066b7a3`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `segment-index-get-live:getHitSync` | `2659570.845 ops/s` | `3281922.532 ops/s` | `+23.40%` | `better` |
| `segment-index-get-live:getMissSync` | `2471200.306 ops/s` | `2072141.072 ops/s` | `-16.15%` | `worse` |
| `segment-index-get-multisegment-cold:getHitSync` | `384042.762 ops/s` | `387665.523 ops/s` | `+0.94%` | `neutral` |
| `segment-index-get-multisegment-cold:getMissSync` | `2633665.618 ops/s` | `2812386.509 ops/s` | `+6.79%` | `better` |
| `segment-index-get-multisegment-hot:getHitSync` | `2244738.417 ops/s` | `2088839.303 ops/s` | `-6.95%` | `warning` |
| `segment-index-get-multisegment-hot:getMissSync` | `2899221.160 ops/s` | `3101235.188 ops/s` | `+6.97%` | `better` |
| `segment-index-get-persisted:getHitSync` | `2254080.224 ops/s` | `2144268.125 ops/s` | `-4.87%` | `warning` |
| `segment-index-get-persisted:getMissSync` | `2719286.939 ops/s` | `2369644.712 ops/s` | `-12.86%` | `worse` |
| `segment-index-hot-route-put:putHotRoute` | `2586479.643 ops/s` | `2785295.846 ops/s` | `+7.69%` | `better` |
| `segment-index-hot-route-put:putThenGetHotRoute` | `1403816.154 ops/s` | `1466321.661 ops/s` | `+4.45%` | `better` |
| `segment-index-lifecycle:openAndCheckAndRepairConsistency` | `87.600 ms/op` | `84.797 ms/op` | `-3.20%` | `warning` |
| `segment-index-lifecycle:openAndCompact` | `98.998 ms/op` | `96.955 ms/op` | `-2.06%` | `neutral` |
| `segment-index-lifecycle:openExisting` | `86.561 ms/op` | `81.914 ms/op` | `-5.37%` | `warning` |
| `segment-index-mixed-drain:partitionedIngestMixed` | `582810.323 ops/s` | `597255.589 ops/s` | `+2.48%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed:getWorkload` | `288797.954 ops/s` | `308554.250 ops/s` | `+6.84%` | `better` |
| `segment-index-mixed-drain:partitionedIngestMixed:putWorkload` | `294012.370 ops/s` | `288701.339 ops/s` | `-1.81%` | `neutral` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed` | `1070732.161 ops/s` | `983944.350 ops/s` | `-8.11%` | `worse` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:getWorkload` | `1022996.064 ops/s` | `935145.945 ops/s` | `-8.59%` | `worse` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:putWorkload` | `47736.096 ops/s` | `48798.405 ops/s` | `+2.23%` | `neutral` |
| `segment-index-persisted-mutation-concurrent:deleteSync` | `13124.107 ops/s` | `12997.046 ops/s` | `-0.97%` | `neutral` |
| `segment-index-persisted-mutation-concurrent:putSync` | `14132.775 ops/s` | `14068.248 ops/s` | `-0.46%` | `neutral` |
| `segment-index-persisted-mutation:deleteSync` | `6356.133 ops/s` | `6385.362 ops/s` | `+0.46%` | `neutral` |
| `segment-index-persisted-mutation:putSync` | `5856.948 ops/s` | `6110.278 ops/s` | `+4.33%` | `better` |
| `segment-index-range-scan:boundedScan` | `18.672 us/op` | `18.035 us/op` | `-3.41%` | `warning` |
| `segment-index-range-scan:fullStreamRangeFallback` | `1341.890 us/op` | `1327.674 us/op` | `-1.06%` | `neutral` |
| `segment-index-range-scan:sequentialRead` | `2648.005 us/op` | `2589.801 us/op` | `-2.20%` | `neutral` |
| `segment-merge-sequential:mergeSequential` | `247.798 us/op` | `239.081 us/op` | `-3.52%` | `warning` |
