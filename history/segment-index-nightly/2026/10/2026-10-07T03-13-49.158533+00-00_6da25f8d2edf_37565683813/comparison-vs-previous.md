# Benchmark Comparison

- Profile: `segment-index-nightly`
- Baseline SHA: `6da25f8d2edfcb35fc090cb0bcf177026066b7a3`
- Candidate SHA: `6da25f8d2edfcb35fc090cb0bcf177026066b7a3`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `segment-index-get-live:getHitSync` | `4834994.375 ops/s` | `4546717.424 ops/s` | `-5.96%` | `warning` |
| `segment-index-get-live:getMissSync` | `4853339.529 ops/s` | `4632176.167 ops/s` | `-4.56%` | `warning` |
| `segment-index-get-multisegment-cold:getHitSync` | `281367.275 ops/s` | `264398.072 ops/s` | `-6.03%` | `warning` |
| `segment-index-get-multisegment-cold:getMissSync` | `4789569.470 ops/s` | `4874860.701 ops/s` | `+1.78%` | `neutral` |
| `segment-index-get-multisegment-hot:getHitSync` | `3441641.625 ops/s` | `3803765.044 ops/s` | `+10.52%` | `better` |
| `segment-index-get-multisegment-hot:getMissSync` | `4174222.891 ops/s` | `4569329.101 ops/s` | `+9.47%` | `better` |
| `segment-index-get-persisted:getHitSync` | `3325800.987 ops/s` | `3397455.713 ops/s` | `+2.15%` | `neutral` |
| `segment-index-get-persisted:getMissSync` | `4722295.779 ops/s` | `4765402.559 ops/s` | `+0.91%` | `neutral` |
| `segment-index-hot-route-put:putHotRoute` | `3828140.597 ops/s` | `4365954.501 ops/s` | `+14.05%` | `better` |
| `segment-index-hot-route-put:putThenGetHotRoute` | `2252910.200 ops/s` | `2277296.722 ops/s` | `+1.08%` | `neutral` |
| `segment-index-lifecycle:openAndCheckAndRepairConsistency` | `244.932 ms/op` | `247.548 ms/op` | `+1.07%` | `neutral` |
| `segment-index-lifecycle:openAndCompact` | `262.072 ms/op` | `261.546 ms/op` | `-0.20%` | `neutral` |
| `segment-index-lifecycle:openExisting` | `241.906 ms/op` | `241.535 ms/op` | `-0.15%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed` | `557205.349 ops/s` | `554887.420 ops/s` | `-0.42%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed:getWorkload` | `269264.914 ops/s` | `279760.553 ops/s` | `+3.90%` | `better` |
| `segment-index-mixed-drain:partitionedIngestMixed:putWorkload` | `287940.435 ops/s` | `275126.867 ops/s` | `-4.45%` | `warning` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed` | `1135199.227 ops/s` | `1308839.318 ops/s` | `+15.30%` | `better` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:getWorkload` | `1099703.343 ops/s` | `1273034.344 ops/s` | `+15.76%` | `better` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:putWorkload` | `35495.884 ops/s` | `35804.974 ops/s` | `+0.87%` | `neutral` |
| `segment-index-persisted-mutation-concurrent:deleteSync` | `8686.246 ops/s` | `8639.310 ops/s` | `-0.54%` | `neutral` |
| `segment-index-persisted-mutation-concurrent:putSync` | `8282.703 ops/s` | `8482.244 ops/s` | `+2.41%` | `neutral` |
| `segment-index-persisted-mutation:deleteSync` | `3018.998 ops/s` | `3191.425 ops/s` | `+5.71%` | `better` |
| `segment-index-persisted-mutation:putSync` | `3169.852 ops/s` | `3070.342 ops/s` | `-3.14%` | `warning` |
| `segment-index-range-scan:boundedScan` | `26.164 us/op` | `30.541 us/op` | `+16.73%` | `better` |
| `segment-index-range-scan:fullStreamRangeFallback` | `2064.635 us/op` | `2021.308 us/op` | `-2.10%` | `neutral` |
| `segment-index-range-scan:sequentialRead` | `3844.410 us/op` | `3801.762 us/op` | `-1.11%` | `neutral` |
| `segment-merge-sequential:mergeSequential` | `333.823 us/op` | `332.546 us/op` | `-0.38%` | `neutral` |
