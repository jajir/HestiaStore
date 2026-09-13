# Benchmark Comparison

- Profile: `segment-index-nightly`
- Baseline SHA: `d3ef04d30386ff556237923d24a98745e3710b30`
- Candidate SHA: `d3ef04d30386ff556237923d24a98745e3710b30`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `segment-index-get-live:getHitSync` | `4775434.661 ops/s` | `4926220.295 ops/s` | `+3.16%` | `better` |
| `segment-index-get-live:getMissSync` | `4659808.604 ops/s` | `4209690.204 ops/s` | `-9.66%` | `worse` |
| `segment-index-get-multisegment-cold:getHitSync` | `272155.497 ops/s` | `273935.112 ops/s` | `+0.65%` | `neutral` |
| `segment-index-get-multisegment-cold:getMissSync` | `4037468.893 ops/s` | `4500573.420 ops/s` | `+11.47%` | `better` |
| `segment-index-get-multisegment-hot:getHitSync` | `3766404.923 ops/s` | `3208255.860 ops/s` | `-14.82%` | `worse` |
| `segment-index-get-multisegment-hot:getMissSync` | `4112741.370 ops/s` | `4056091.412 ops/s` | `-1.38%` | `neutral` |
| `segment-index-get-persisted:getHitSync` | `3558684.236 ops/s` | `3285118.111 ops/s` | `-7.69%` | `worse` |
| `segment-index-get-persisted:getMissSync` | `4705293.479 ops/s` | `4424344.366 ops/s` | `-5.97%` | `warning` |
| `segment-index-hot-route-put:putHotRoute` | `4277453.342 ops/s` | `4005112.485 ops/s` | `-6.37%` | `warning` |
| `segment-index-hot-route-put:putThenGetHotRoute` | `2177668.247 ops/s` | `2230352.014 ops/s` | `+2.42%` | `neutral` |
| `segment-index-lifecycle:openAndCheckAndRepairConsistency` | `278.856 ms/op` | `277.524 ms/op` | `-0.48%` | `neutral` |
| `segment-index-lifecycle:openAndCompact` | `301.572 ms/op` | `298.630 ms/op` | `-0.98%` | `neutral` |
| `segment-index-lifecycle:openExisting` | `274.295 ms/op` | `273.697 ms/op` | `-0.22%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed` | `558967.981 ops/s` | `542638.778 ops/s` | `-2.92%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed:getWorkload` | `274630.572 ops/s` | `264737.653 ops/s` | `-3.60%` | `warning` |
| `segment-index-mixed-drain:partitionedIngestMixed:putWorkload` | `284337.409 ops/s` | `277901.125 ops/s` | `-2.26%` | `neutral` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed` | `1286324.330 ops/s` | `1253565.872 ops/s` | `-2.55%` | `neutral` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:getWorkload` | `1266789.090 ops/s` | `1233245.460 ops/s` | `-2.65%` | `neutral` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:putWorkload` | `19535.239 ops/s` | `20320.412 ops/s` | `+4.02%` | `better` |
| `segment-index-persisted-mutation-concurrent:deleteSync` | `8161.992 ops/s` | `7760.894 ops/s` | `-4.91%` | `warning` |
| `segment-index-persisted-mutation-concurrent:putSync` | `8114.696 ops/s` | `7133.258 ops/s` | `-12.09%` | `worse` |
| `segment-index-persisted-mutation:deleteSync` | `3281.184 ops/s` | `3088.883 ops/s` | `-5.86%` | `warning` |
| `segment-index-persisted-mutation:putSync` | `3183.280 ops/s` | `2966.203 ops/s` | `-6.82%` | `warning` |
| `segment-index-range-scan:boundedScan` | `32.046 us/op` | `27.990 us/op` | `-12.66%` | `worse` |
| `segment-index-range-scan:fullStreamRangeFallback` | `2119.844 us/op` | `2178.898 us/op` | `+2.79%` | `neutral` |
| `segment-index-range-scan:sequentialRead` | `4147.615 us/op` | `4104.370 us/op` | `-1.04%` | `neutral` |
| `segment-merge-sequential:mergeSequential` | `358.482 us/op` | `358.439 us/op` | `-0.01%` | `neutral` |
