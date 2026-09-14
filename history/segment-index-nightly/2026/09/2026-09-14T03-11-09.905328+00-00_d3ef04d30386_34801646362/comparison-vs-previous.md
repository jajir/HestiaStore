# Benchmark Comparison

- Profile: `segment-index-nightly`
- Baseline SHA: `d3ef04d30386ff556237923d24a98745e3710b30`
- Candidate SHA: `d3ef04d30386ff556237923d24a98745e3710b30`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `segment-index-get-live:getHitSync` | `4926220.295 ops/s` | `4548920.285 ops/s` | `-7.66%` | `worse` |
| `segment-index-get-live:getMissSync` | `4209690.204 ops/s` | `4156093.556 ops/s` | `-1.27%` | `neutral` |
| `segment-index-get-multisegment-cold:getHitSync` | `273935.112 ops/s` | `285023.108 ops/s` | `+4.05%` | `better` |
| `segment-index-get-multisegment-cold:getMissSync` | `4500573.420 ops/s` | `4475000.164 ops/s` | `-0.57%` | `neutral` |
| `segment-index-get-multisegment-hot:getHitSync` | `3208255.860 ops/s` | `3713231.315 ops/s` | `+15.74%` | `better` |
| `segment-index-get-multisegment-hot:getMissSync` | `4056091.412 ops/s` | `4673586.779 ops/s` | `+15.22%` | `better` |
| `segment-index-get-persisted:getHitSync` | `3285118.111 ops/s` | `3171293.653 ops/s` | `-3.46%` | `warning` |
| `segment-index-get-persisted:getMissSync` | `4424344.366 ops/s` | `4256792.967 ops/s` | `-3.79%` | `warning` |
| `segment-index-hot-route-put:putHotRoute` | `4005112.485 ops/s` | `4195077.437 ops/s` | `+4.74%` | `better` |
| `segment-index-hot-route-put:putThenGetHotRoute` | `2230352.014 ops/s` | `2226671.333 ops/s` | `-0.17%` | `neutral` |
| `segment-index-lifecycle:openAndCheckAndRepairConsistency` | `277.524 ms/op` | `239.992 ms/op` | `-13.52%` | `worse` |
| `segment-index-lifecycle:openAndCompact` | `298.630 ms/op` | `261.161 ms/op` | `-12.55%` | `worse` |
| `segment-index-lifecycle:openExisting` | `273.697 ms/op` | `237.398 ms/op` | `-13.26%` | `worse` |
| `segment-index-mixed-drain:partitionedIngestMixed` | `542638.778 ops/s` | `522792.738 ops/s` | `-3.66%` | `warning` |
| `segment-index-mixed-drain:partitionedIngestMixed:getWorkload` | `264737.653 ops/s` | `254373.954 ops/s` | `-3.91%` | `warning` |
| `segment-index-mixed-drain:partitionedIngestMixed:putWorkload` | `277901.125 ops/s` | `268418.784 ops/s` | `-3.41%` | `warning` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed` | `1253565.872 ops/s` | `1236159.909 ops/s` | `-1.39%` | `neutral` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:getWorkload` | `1233245.460 ops/s` | `1214464.948 ops/s` | `-1.52%` | `neutral` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:putWorkload` | `20320.412 ops/s` | `21694.962 ops/s` | `+6.76%` | `better` |
| `segment-index-persisted-mutation-concurrent:deleteSync` | `7760.894 ops/s` | `7030.232 ops/s` | `-9.41%` | `worse` |
| `segment-index-persisted-mutation-concurrent:putSync` | `7133.258 ops/s` | `6817.505 ops/s` | `-4.43%` | `warning` |
| `segment-index-persisted-mutation:deleteSync` | `3088.883 ops/s` | `2289.352 ops/s` | `-25.88%` | `worse` |
| `segment-index-persisted-mutation:putSync` | `2966.203 ops/s` | `2346.081 ops/s` | `-20.91%` | `worse` |
| `segment-index-range-scan:boundedScan` | `27.990 us/op` | `30.782 us/op` | `+9.97%` | `better` |
| `segment-index-range-scan:fullStreamRangeFallback` | `2178.898 us/op` | `2007.861 us/op` | `-7.85%` | `worse` |
| `segment-index-range-scan:sequentialRead` | `4104.370 us/op` | `3897.893 us/op` | `-5.03%` | `warning` |
| `segment-merge-sequential:mergeSequential` | `358.439 us/op` | `334.279 us/op` | `-6.74%` | `warning` |
