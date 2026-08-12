# Benchmark Comparison

- Profile: `segment-index-nightly`
- Baseline SHA: `7c9b9709d7b4925f77416d396930aabcca6a6987`
- Candidate SHA: `7c9b9709d7b4925f77416d396930aabcca6a6987`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `segment-index-get-live:getHitSync` | `4439463.820 ops/s` | `4851639.172 ops/s` | `+9.28%` | `better` |
| `segment-index-get-live:getMissSync` | `4396206.523 ops/s` | `4977899.984 ops/s` | `+13.23%` | `better` |
| `segment-index-get-multisegment-cold:getHitSync` | `275575.436 ops/s` | `273864.490 ops/s` | `-0.62%` | `neutral` |
| `segment-index-get-multisegment-cold:getMissSync` | `4607073.005 ops/s` | `4434252.725 ops/s` | `-3.75%` | `warning` |
| `segment-index-get-multisegment-hot:getHitSync` | `3543110.550 ops/s` | `3573434.641 ops/s` | `+0.86%` | `neutral` |
| `segment-index-get-multisegment-hot:getMissSync` | `4627303.421 ops/s` | `4924857.520 ops/s` | `+6.43%` | `better` |
| `segment-index-get-persisted:getHitSync` | `3104239.374 ops/s` | `3533020.119 ops/s` | `+13.81%` | `better` |
| `segment-index-get-persisted:getMissSync` | `4614382.344 ops/s` | `5032718.343 ops/s` | `+9.07%` | `better` |
| `segment-index-hot-route-put:putHotRoute` | `3905245.220 ops/s` | `4374878.573 ops/s` | `+12.03%` | `better` |
| `segment-index-hot-route-put:putThenGetHotRoute` | `2252010.076 ops/s` | `2358188.273 ops/s` | `+4.71%` | `better` |
| `segment-index-lifecycle:openAndCheckAndRepairConsistency` | `246.255 ms/op` | `242.829 ms/op` | `-1.39%` | `neutral` |
| `segment-index-lifecycle:openAndCompact` | `274.383 ms/op` | `265.219 ms/op` | `-3.34%` | `warning` |
| `segment-index-lifecycle:openExisting` | `244.536 ms/op` | `238.304 ms/op` | `-2.55%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed` | `504952.118 ops/s` | `526584.784 ops/s` | `+4.28%` | `better` |
| `segment-index-mixed-drain:partitionedIngestMixed:getWorkload` | `257153.300 ops/s` | `281726.532 ops/s` | `+9.56%` | `better` |
| `segment-index-mixed-drain:partitionedIngestMixed:putWorkload` | `247798.818 ops/s` | `244858.252 ops/s` | `-1.19%` | `neutral` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed` | `1150662.426 ops/s` | `1188723.288 ops/s` | `+3.31%` | `better` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:getWorkload` | `1136074.141 ops/s` | `1174694.524 ops/s` | `+3.40%` | `better` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:putWorkload` | `14588.286 ops/s` | `14028.764 ops/s` | `-3.84%` | `warning` |
| `segment-index-persisted-mutation-concurrent:deleteSync` | `6793.978 ops/s` | `6934.370 ops/s` | `+2.07%` | `neutral` |
| `segment-index-persisted-mutation-concurrent:putSync` | `6766.163 ops/s` | `6939.033 ops/s` | `+2.55%` | `neutral` |
| `segment-index-persisted-mutation:deleteSync` | `2347.376 ops/s` | `2320.608 ops/s` | `-1.14%` | `neutral` |
| `segment-index-persisted-mutation:putSync` | `2268.622 ops/s` | `2253.949 ops/s` | `-0.65%` | `neutral` |
| `segment-index-range-scan:boundedScan` | `34.410 us/op` | `27.991 us/op` | `-18.66%` | `worse` |
| `segment-index-range-scan:fullStreamRangeFallback` | `2053.197 us/op` | `2042.206 us/op` | `-0.54%` | `neutral` |
| `segment-index-range-scan:sequentialRead` | `3992.064 us/op` | `3931.468 us/op` | `-1.52%` | `neutral` |
| `segment-merge-sequential:mergeSequential` | `332.621 us/op` | `331.434 us/op` | `-0.36%` | `neutral` |
