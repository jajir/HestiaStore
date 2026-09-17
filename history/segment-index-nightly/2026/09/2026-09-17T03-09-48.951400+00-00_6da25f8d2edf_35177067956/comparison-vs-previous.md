# Benchmark Comparison

- Profile: `segment-index-nightly`
- Baseline SHA: `6da25f8d2edfcb35fc090cb0bcf177026066b7a3`
- Candidate SHA: `6da25f8d2edfcb35fc090cb0bcf177026066b7a3`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `segment-index-get-live:getHitSync` | `5203433.097 ops/s` | `4762548.046 ops/s` | `-8.47%` | `worse` |
| `segment-index-get-live:getMissSync` | `4702526.253 ops/s` | `4764029.322 ops/s` | `+1.31%` | `neutral` |
| `segment-index-get-multisegment-cold:getHitSync` | `284511.093 ops/s` | `283245.173 ops/s` | `-0.44%` | `neutral` |
| `segment-index-get-multisegment-cold:getMissSync` | `4105603.269 ops/s` | `4280679.902 ops/s` | `+4.26%` | `better` |
| `segment-index-get-multisegment-hot:getHitSync` | `3681453.859 ops/s` | `3641114.588 ops/s` | `-1.10%` | `neutral` |
| `segment-index-get-multisegment-hot:getMissSync` | `4907946.521 ops/s` | `4569377.372 ops/s` | `-6.90%` | `warning` |
| `segment-index-get-persisted:getHitSync` | `3402017.417 ops/s` | `3451825.773 ops/s` | `+1.46%` | `neutral` |
| `segment-index-get-persisted:getMissSync` | `4438726.528 ops/s` | `4078328.579 ops/s` | `-8.12%` | `worse` |
| `segment-index-hot-route-put:putHotRoute` | `4247482.965 ops/s` | `4168730.469 ops/s` | `-1.85%` | `neutral` |
| `segment-index-hot-route-put:putThenGetHotRoute` | `2384535.304 ops/s` | `2273197.967 ops/s` | `-4.67%` | `warning` |
| `segment-index-lifecycle:openAndCheckAndRepairConsistency` | `245.100 ms/op` | `278.113 ms/op` | `+13.47%` | `better` |
| `segment-index-lifecycle:openAndCompact` | `262.460 ms/op` | `294.903 ms/op` | `+12.36%` | `better` |
| `segment-index-lifecycle:openExisting` | `240.373 ms/op` | `272.302 ms/op` | `+13.28%` | `better` |
| `segment-index-mixed-drain:partitionedIngestMixed` | `553857.174 ops/s` | `566047.803 ops/s` | `+2.20%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed:getWorkload` | `261202.616 ops/s` | `271169.357 ops/s` | `+3.82%` | `better` |
| `segment-index-mixed-drain:partitionedIngestMixed:putWorkload` | `292654.558 ops/s` | `294878.446 ops/s` | `+0.76%` | `neutral` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed` | `1268361.602 ops/s` | `1213027.025 ops/s` | `-4.36%` | `warning` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:getWorkload` | `1234154.893 ops/s` | `1177478.007 ops/s` | `-4.59%` | `warning` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:putWorkload` | `34206.708 ops/s` | `35549.019 ops/s` | `+3.92%` | `better` |
| `segment-index-persisted-mutation-concurrent:deleteSync` | `7076.578 ops/s` | `7413.610 ops/s` | `+4.76%` | `better` |
| `segment-index-persisted-mutation-concurrent:putSync` | `7143.815 ops/s` | `7476.965 ops/s` | `+4.66%` | `better` |
| `segment-index-persisted-mutation:deleteSync` | `2627.484 ops/s` | `3340.354 ops/s` | `+27.13%` | `better` |
| `segment-index-persisted-mutation:putSync` | `2581.302 ops/s` | `3233.038 ops/s` | `+25.25%` | `better` |
| `segment-index-range-scan:boundedScan` | `26.636 us/op` | `31.600 us/op` | `+18.63%` | `better` |
| `segment-index-range-scan:fullStreamRangeFallback` | `2000.828 us/op` | `2129.262 us/op` | `+6.42%` | `better` |
| `segment-index-range-scan:sequentialRead` | `3866.422 us/op` | `4018.089 us/op` | `+3.92%` | `better` |
| `segment-merge-sequential:mergeSequential` | `335.278 us/op` | `358.904 us/op` | `+7.05%` | `better` |
