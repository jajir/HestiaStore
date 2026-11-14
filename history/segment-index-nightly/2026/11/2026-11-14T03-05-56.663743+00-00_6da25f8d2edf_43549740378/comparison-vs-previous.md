# Benchmark Comparison

- Profile: `segment-index-nightly`
- Baseline SHA: `6da25f8d2edfcb35fc090cb0bcf177026066b7a3`
- Candidate SHA: `6da25f8d2edfcb35fc090cb0bcf177026066b7a3`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `segment-index-get-live:getHitSync` | `6172802.003 ops/s` | `2659570.845 ops/s` | `-56.91%` | `worse` |
| `segment-index-get-live:getMissSync` | `6810165.513 ops/s` | `2471200.306 ops/s` | `-63.71%` | `worse` |
| `segment-index-get-multisegment-cold:getHitSync` | `492855.741 ops/s` | `384042.762 ops/s` | `-22.08%` | `worse` |
| `segment-index-get-multisegment-cold:getMissSync` | `6683357.990 ops/s` | `2633665.618 ops/s` | `-60.59%` | `worse` |
| `segment-index-get-multisegment-hot:getHitSync` | `5755332.908 ops/s` | `2244738.417 ops/s` | `-61.00%` | `worse` |
| `segment-index-get-multisegment-hot:getMissSync` | `6307124.842 ops/s` | `2899221.160 ops/s` | `-54.03%` | `worse` |
| `segment-index-get-persisted:getHitSync` | `5355417.439 ops/s` | `2254080.224 ops/s` | `-57.91%` | `worse` |
| `segment-index-get-persisted:getMissSync` | `6349015.030 ops/s` | `2719286.939 ops/s` | `-57.17%` | `worse` |
| `segment-index-hot-route-put:putHotRoute` | `6502628.734 ops/s` | `2586479.643 ops/s` | `-60.22%` | `worse` |
| `segment-index-hot-route-put:putThenGetHotRoute` | `3118690.872 ops/s` | `1403816.154 ops/s` | `-54.99%` | `worse` |
| `segment-index-lifecycle:openAndCheckAndRepairConsistency` | `182.918 ms/op` | `87.600 ms/op` | `-52.11%` | `worse` |
| `segment-index-lifecycle:openAndCompact` | `198.138 ms/op` | `98.998 ms/op` | `-50.04%` | `worse` |
| `segment-index-lifecycle:openExisting` | `182.634 ms/op` | `86.561 ms/op` | `-52.60%` | `worse` |
| `segment-index-mixed-drain:partitionedIngestMixed` | `748029.418 ops/s` | `582810.323 ops/s` | `-22.09%` | `worse` |
| `segment-index-mixed-drain:partitionedIngestMixed:getWorkload` | `459580.487 ops/s` | `288797.954 ops/s` | `-37.16%` | `worse` |
| `segment-index-mixed-drain:partitionedIngestMixed:putWorkload` | `288448.931 ops/s` | `294012.370 ops/s` | `+1.93%` | `neutral` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed` | `1913959.189 ops/s` | `1070732.161 ops/s` | `-44.06%` | `worse` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:getWorkload` | `1862872.400 ops/s` | `1022996.064 ops/s` | `-45.09%` | `worse` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:putWorkload` | `51086.789 ops/s` | `47736.096 ops/s` | `-6.56%` | `warning` |
| `segment-index-persisted-mutation-concurrent:deleteSync` | `8088.126 ops/s` | `13124.107 ops/s` | `+62.26%` | `better` |
| `segment-index-persisted-mutation-concurrent:putSync` | `7986.556 ops/s` | `14132.775 ops/s` | `+76.96%` | `better` |
| `segment-index-persisted-mutation:deleteSync` | `3832.690 ops/s` | `6356.133 ops/s` | `+65.84%` | `better` |
| `segment-index-persisted-mutation:putSync` | `3683.267 ops/s` | `5856.948 ops/s` | `+59.02%` | `better` |
| `segment-index-range-scan:boundedScan` | `19.842 us/op` | `18.672 us/op` | `-5.90%` | `warning` |
| `segment-index-range-scan:fullStreamRangeFallback` | `1190.691 us/op` | `1341.890 us/op` | `+12.70%` | `better` |
| `segment-index-range-scan:sequentialRead` | `2299.304 us/op` | `2648.005 us/op` | `+15.17%` | `better` |
| `segment-merge-sequential:mergeSequential` | `182.242 us/op` | `247.798 us/op` | `+35.97%` | `better` |
