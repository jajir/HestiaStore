# Benchmark Comparison

- Profile: `segment-index-nightly`
- Baseline SHA: `6da25f8d2edfcb35fc090cb0bcf177026066b7a3`
- Candidate SHA: `6da25f8d2edfcb35fc090cb0bcf177026066b7a3`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `segment-index-get-live:getHitSync` | `4896587.860 ops/s` | `5330470.480 ops/s` | `+8.86%` | `better` |
| `segment-index-get-live:getMissSync` | `4878281.887 ops/s` | `4518270.404 ops/s` | `-7.38%` | `worse` |
| `segment-index-get-multisegment-cold:getHitSync` | `267162.589 ops/s` | `273846.817 ops/s` | `+2.50%` | `neutral` |
| `segment-index-get-multisegment-cold:getMissSync` | `4071738.635 ops/s` | `4576067.311 ops/s` | `+12.39%` | `better` |
| `segment-index-get-multisegment-hot:getHitSync` | `3667237.194 ops/s` | `3627008.601 ops/s` | `-1.10%` | `neutral` |
| `segment-index-get-multisegment-hot:getMissSync` | `4529659.457 ops/s` | `4575331.797 ops/s` | `+1.01%` | `neutral` |
| `segment-index-get-persisted:getHitSync` | `3355764.485 ops/s` | `3229149.011 ops/s` | `-3.77%` | `warning` |
| `segment-index-get-persisted:getMissSync` | `4521425.595 ops/s` | `4771545.877 ops/s` | `+5.53%` | `better` |
| `segment-index-hot-route-put:putHotRoute` | `4356209.180 ops/s` | `4478846.662 ops/s` | `+2.82%` | `neutral` |
| `segment-index-hot-route-put:putThenGetHotRoute` | `2304195.196 ops/s` | `2333173.471 ops/s` | `+1.26%` | `neutral` |
| `segment-index-lifecycle:openAndCheckAndRepairConsistency` | `246.457 ms/op` | `237.804 ms/op` | `-3.51%` | `warning` |
| `segment-index-lifecycle:openAndCompact` | `260.040 ms/op` | `255.266 ms/op` | `-1.84%` | `neutral` |
| `segment-index-lifecycle:openExisting` | `240.325 ms/op` | `237.804 ms/op` | `-1.05%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed` | `534409.120 ops/s` | `576296.345 ops/s` | `+7.84%` | `better` |
| `segment-index-mixed-drain:partitionedIngestMixed:getWorkload` | `240225.055 ops/s` | `274303.089 ops/s` | `+14.19%` | `better` |
| `segment-index-mixed-drain:partitionedIngestMixed:putWorkload` | `294184.065 ops/s` | `301993.256 ops/s` | `+2.65%` | `neutral` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed` | `1291305.269 ops/s` | `1287846.497 ops/s` | `-0.27%` | `neutral` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:getWorkload` | `1254864.837 ops/s` | `1253655.838 ops/s` | `-0.10%` | `neutral` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:putWorkload` | `36440.432 ops/s` | `34190.659 ops/s` | `-6.17%` | `warning` |
| `segment-index-persisted-mutation-concurrent:deleteSync` | `8648.055 ops/s` | `8908.289 ops/s` | `+3.01%` | `better` |
| `segment-index-persisted-mutation-concurrent:putSync` | `8491.703 ops/s` | `8860.223 ops/s` | `+4.34%` | `better` |
| `segment-index-persisted-mutation:deleteSync` | `3245.642 ops/s` | `3178.742 ops/s` | `-2.06%` | `neutral` |
| `segment-index-persisted-mutation:putSync` | `3066.086 ops/s` | `3150.647 ops/s` | `+2.76%` | `neutral` |
| `segment-index-range-scan:boundedScan` | `29.062 us/op` | `28.026 us/op` | `-3.56%` | `warning` |
| `segment-index-range-scan:fullStreamRangeFallback` | `1992.386 us/op` | `1973.586 us/op` | `-0.94%` | `neutral` |
| `segment-index-range-scan:sequentialRead` | `3908.676 us/op` | `3881.458 us/op` | `-0.70%` | `neutral` |
| `segment-merge-sequential:mergeSequential` | `333.392 us/op` | `332.591 us/op` | `-0.24%` | `neutral` |
