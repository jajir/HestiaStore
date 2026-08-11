# Benchmark Comparison

- Profile: `segment-index-nightly`
- Baseline SHA: `7c9b9709d7b4925f77416d396930aabcca6a6987`
- Candidate SHA: `7c9b9709d7b4925f77416d396930aabcca6a6987`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `segment-index-get-live:getHitSync` | `5571604.693 ops/s` | `4439463.820 ops/s` | `-20.32%` | `worse` |
| `segment-index-get-live:getMissSync` | `4734810.560 ops/s` | `4396206.523 ops/s` | `-7.15%` | `worse` |
| `segment-index-get-multisegment-cold:getHitSync` | `254528.455 ops/s` | `275575.436 ops/s` | `+8.27%` | `better` |
| `segment-index-get-multisegment-cold:getMissSync` | `4296622.869 ops/s` | `4607073.005 ops/s` | `+7.23%` | `better` |
| `segment-index-get-multisegment-hot:getHitSync` | `3581300.486 ops/s` | `3543110.550 ops/s` | `-1.07%` | `neutral` |
| `segment-index-get-multisegment-hot:getMissSync` | `4708509.777 ops/s` | `4627303.421 ops/s` | `-1.72%` | `neutral` |
| `segment-index-get-persisted:getHitSync` | `3559823.974 ops/s` | `3104239.374 ops/s` | `-12.80%` | `worse` |
| `segment-index-get-persisted:getMissSync` | `4630270.577 ops/s` | `4614382.344 ops/s` | `-0.34%` | `neutral` |
| `segment-index-hot-route-put:putHotRoute` | `4331201.523 ops/s` | `3905245.220 ops/s` | `-9.83%` | `worse` |
| `segment-index-hot-route-put:putThenGetHotRoute` | `2209350.305 ops/s` | `2252010.076 ops/s` | `+1.93%` | `neutral` |
| `segment-index-lifecycle:openAndCheckAndRepairConsistency` | `248.984 ms/op` | `246.255 ms/op` | `-1.10%` | `neutral` |
| `segment-index-lifecycle:openAndCompact` | `272.255 ms/op` | `274.383 ms/op` | `+0.78%` | `neutral` |
| `segment-index-lifecycle:openExisting` | `243.756 ms/op` | `244.536 ms/op` | `+0.32%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed` | `526361.696 ops/s` | `504952.118 ops/s` | `-4.07%` | `warning` |
| `segment-index-mixed-drain:partitionedIngestMixed:getWorkload` | `284661.103 ops/s` | `257153.300 ops/s` | `-9.66%` | `worse` |
| `segment-index-mixed-drain:partitionedIngestMixed:putWorkload` | `241700.593 ops/s` | `247798.818 ops/s` | `+2.52%` | `neutral` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed` | `1172816.844 ops/s` | `1150662.426 ops/s` | `-1.89%` | `neutral` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:getWorkload` | `1157918.256 ops/s` | `1136074.141 ops/s` | `-1.89%` | `neutral` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:putWorkload` | `14898.589 ops/s` | `14588.286 ops/s` | `-2.08%` | `neutral` |
| `segment-index-persisted-mutation-concurrent:deleteSync` | `6704.350 ops/s` | `6793.978 ops/s` | `+1.34%` | `neutral` |
| `segment-index-persisted-mutation-concurrent:putSync` | `6821.582 ops/s` | `6766.163 ops/s` | `-0.81%` | `neutral` |
| `segment-index-persisted-mutation:deleteSync` | `2230.098 ops/s` | `2347.376 ops/s` | `+5.26%` | `better` |
| `segment-index-persisted-mutation:putSync` | `2231.054 ops/s` | `2268.622 ops/s` | `+1.68%` | `neutral` |
| `segment-index-range-scan:boundedScan` | `36.997 us/op` | `34.410 us/op` | `-6.99%` | `warning` |
| `segment-index-range-scan:fullStreamRangeFallback` | `2030.371 us/op` | `2053.197 us/op` | `+1.12%` | `neutral` |
| `segment-index-range-scan:sequentialRead` | `3966.093 us/op` | `3992.064 us/op` | `+0.65%` | `neutral` |
| `segment-merge-sequential:mergeSequential` | `335.544 us/op` | `332.621 us/op` | `-0.87%` | `neutral` |
