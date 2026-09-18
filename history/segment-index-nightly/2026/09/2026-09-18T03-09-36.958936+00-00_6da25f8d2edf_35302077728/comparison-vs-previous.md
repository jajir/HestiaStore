# Benchmark Comparison

- Profile: `segment-index-nightly`
- Baseline SHA: `6da25f8d2edfcb35fc090cb0bcf177026066b7a3`
- Candidate SHA: `6da25f8d2edfcb35fc090cb0bcf177026066b7a3`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `segment-index-get-live:getHitSync` | `4762548.046 ops/s` | `4570328.413 ops/s` | `-4.04%` | `warning` |
| `segment-index-get-live:getMissSync` | `4764029.322 ops/s` | `4819524.501 ops/s` | `+1.16%` | `neutral` |
| `segment-index-get-multisegment-cold:getHitSync` | `283245.173 ops/s` | `271516.158 ops/s` | `-4.14%` | `warning` |
| `segment-index-get-multisegment-cold:getMissSync` | `4280679.902 ops/s` | `4901923.213 ops/s` | `+14.51%` | `better` |
| `segment-index-get-multisegment-hot:getHitSync` | `3641114.588 ops/s` | `3504695.714 ops/s` | `-3.75%` | `warning` |
| `segment-index-get-multisegment-hot:getMissSync` | `4569377.372 ops/s` | `4419793.553 ops/s` | `-3.27%` | `warning` |
| `segment-index-get-persisted:getHitSync` | `3451825.773 ops/s` | `3490357.312 ops/s` | `+1.12%` | `neutral` |
| `segment-index-get-persisted:getMissSync` | `4078328.579 ops/s` | `4586927.537 ops/s` | `+12.47%` | `better` |
| `segment-index-hot-route-put:putHotRoute` | `4168730.469 ops/s` | `4498626.545 ops/s` | `+7.91%` | `better` |
| `segment-index-hot-route-put:putThenGetHotRoute` | `2273197.967 ops/s` | `2237791.684 ops/s` | `-1.56%` | `neutral` |
| `segment-index-lifecycle:openAndCheckAndRepairConsistency` | `278.113 ms/op` | `245.425 ms/op` | `-11.75%` | `worse` |
| `segment-index-lifecycle:openAndCompact` | `294.903 ms/op` | `259.060 ms/op` | `-12.15%` | `worse` |
| `segment-index-lifecycle:openExisting` | `272.302 ms/op` | `242.243 ms/op` | `-11.04%` | `worse` |
| `segment-index-mixed-drain:partitionedIngestMixed` | `566047.803 ops/s` | `550011.892 ops/s` | `-2.83%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed:getWorkload` | `271169.357 ops/s` | `258586.263 ops/s` | `-4.64%` | `warning` |
| `segment-index-mixed-drain:partitionedIngestMixed:putWorkload` | `294878.446 ops/s` | `291425.630 ops/s` | `-1.17%` | `neutral` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed` | `1213027.025 ops/s` | `1215560.545 ops/s` | `+0.21%` | `neutral` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:getWorkload` | `1177478.007 ops/s` | `1180648.845 ops/s` | `+0.27%` | `neutral` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:putWorkload` | `35549.019 ops/s` | `34911.700 ops/s` | `-1.79%` | `neutral` |
| `segment-index-persisted-mutation-concurrent:deleteSync` | `7413.610 ops/s` | `6817.221 ops/s` | `-8.04%` | `worse` |
| `segment-index-persisted-mutation-concurrent:putSync` | `7476.965 ops/s` | `7183.502 ops/s` | `-3.92%` | `warning` |
| `segment-index-persisted-mutation:deleteSync` | `3340.354 ops/s` | `2642.936 ops/s` | `-20.88%` | `worse` |
| `segment-index-persisted-mutation:putSync` | `3233.038 ops/s` | `2562.378 ops/s` | `-20.74%` | `worse` |
| `segment-index-range-scan:boundedScan` | `31.600 us/op` | `26.033 us/op` | `-17.62%` | `worse` |
| `segment-index-range-scan:fullStreamRangeFallback` | `2129.262 us/op` | `2013.326 us/op` | `-5.44%` | `warning` |
| `segment-index-range-scan:sequentialRead` | `4018.089 us/op` | `3885.004 us/op` | `-3.31%` | `warning` |
| `segment-merge-sequential:mergeSequential` | `358.904 us/op` | `333.035 us/op` | `-7.21%` | `worse` |
