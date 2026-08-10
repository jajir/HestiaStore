# Benchmark Comparison

- Profile: `segment-index-nightly`
- Baseline SHA: `7c9b9709d7b4925f77416d396930aabcca6a6987`
- Candidate SHA: `7c9b9709d7b4925f77416d396930aabcca6a6987`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `segment-index-get-live:getHitSync` | `5098058.387 ops/s` | `5571604.693 ops/s` | `+9.29%` | `better` |
| `segment-index-get-live:getMissSync` | `4567428.518 ops/s` | `4734810.560 ops/s` | `+3.66%` | `better` |
| `segment-index-get-multisegment-cold:getHitSync` | `274447.209 ops/s` | `254528.455 ops/s` | `-7.26%` | `worse` |
| `segment-index-get-multisegment-cold:getMissSync` | `4451888.907 ops/s` | `4296622.869 ops/s` | `-3.49%` | `warning` |
| `segment-index-get-multisegment-hot:getHitSync` | `3575068.512 ops/s` | `3581300.486 ops/s` | `+0.17%` | `neutral` |
| `segment-index-get-multisegment-hot:getMissSync` | `4418239.207 ops/s` | `4708509.777 ops/s` | `+6.57%` | `better` |
| `segment-index-get-persisted:getHitSync` | `3374481.096 ops/s` | `3559823.974 ops/s` | `+5.49%` | `better` |
| `segment-index-get-persisted:getMissSync` | `4724481.118 ops/s` | `4630270.577 ops/s` | `-1.99%` | `neutral` |
| `segment-index-hot-route-put:putHotRoute` | `4102574.383 ops/s` | `4331201.523 ops/s` | `+5.57%` | `better` |
| `segment-index-hot-route-put:putThenGetHotRoute` | `2348711.610 ops/s` | `2209350.305 ops/s` | `-5.93%` | `warning` |
| `segment-index-lifecycle:openAndCheckAndRepairConsistency` | `248.498 ms/op` | `248.984 ms/op` | `+0.20%` | `neutral` |
| `segment-index-lifecycle:openAndCompact` | `271.075 ms/op` | `272.255 ms/op` | `+0.44%` | `neutral` |
| `segment-index-lifecycle:openExisting` | `241.993 ms/op` | `243.756 ms/op` | `+0.73%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed` | `529647.596 ops/s` | `526361.696 ops/s` | `-0.62%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed:getWorkload` | `284597.767 ops/s` | `284661.103 ops/s` | `+0.02%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed:putWorkload` | `245049.829 ops/s` | `241700.593 ops/s` | `-1.37%` | `neutral` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed` | `1132402.193 ops/s` | `1172816.844 ops/s` | `+3.57%` | `better` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:getWorkload` | `1117418.936 ops/s` | `1157918.256 ops/s` | `+3.62%` | `better` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:putWorkload` | `14983.257 ops/s` | `14898.589 ops/s` | `-0.57%` | `neutral` |
| `segment-index-persisted-mutation-concurrent:deleteSync` | `6910.060 ops/s` | `6704.350 ops/s` | `-2.98%` | `neutral` |
| `segment-index-persisted-mutation-concurrent:putSync` | `6807.998 ops/s` | `6821.582 ops/s` | `+0.20%` | `neutral` |
| `segment-index-persisted-mutation:deleteSync` | `2340.191 ops/s` | `2230.098 ops/s` | `-4.70%` | `warning` |
| `segment-index-persisted-mutation:putSync` | `2270.842 ops/s` | `2231.054 ops/s` | `-1.75%` | `neutral` |
| `segment-index-range-scan:boundedScan` | `27.479 us/op` | `36.997 us/op` | `+34.64%` | `better` |
| `segment-index-range-scan:fullStreamRangeFallback` | `2078.504 us/op` | `2030.371 us/op` | `-2.32%` | `neutral` |
| `segment-index-range-scan:sequentialRead` | `4218.826 us/op` | `3966.093 us/op` | `-5.99%` | `warning` |
| `segment-merge-sequential:mergeSequential` | `331.177 us/op` | `335.544 us/op` | `+1.32%` | `neutral` |
