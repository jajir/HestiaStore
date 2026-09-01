# Benchmark Comparison

- Profile: `segment-index-nightly`
- Baseline SHA: `d3ef04d30386ff556237923d24a98745e3710b30`
- Candidate SHA: `d3ef04d30386ff556237923d24a98745e3710b30`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `segment-index-get-live:getHitSync` | `5023657.148 ops/s` | `5541133.745 ops/s` | `+10.30%` | `better` |
| `segment-index-get-live:getMissSync` | `4305297.436 ops/s` | `4503061.362 ops/s` | `+4.59%` | `better` |
| `segment-index-get-multisegment-cold:getHitSync` | `261843.794 ops/s` | `277052.856 ops/s` | `+5.81%` | `better` |
| `segment-index-get-multisegment-cold:getMissSync` | `4246049.278 ops/s` | `4592319.531 ops/s` | `+8.16%` | `better` |
| `segment-index-get-multisegment-hot:getHitSync` | `3304150.564 ops/s` | `3531374.692 ops/s` | `+6.88%` | `better` |
| `segment-index-get-multisegment-hot:getMissSync` | `4569602.691 ops/s` | `4252990.382 ops/s` | `-6.93%` | `warning` |
| `segment-index-get-persisted:getHitSync` | `3145932.929 ops/s` | `3379933.278 ops/s` | `+7.44%` | `better` |
| `segment-index-get-persisted:getMissSync` | `4469912.064 ops/s` | `4864101.458 ops/s` | `+8.82%` | `better` |
| `segment-index-hot-route-put:putHotRoute` | `3985728.614 ops/s` | `4372003.459 ops/s` | `+9.69%` | `better` |
| `segment-index-hot-route-put:putThenGetHotRoute` | `1941320.251 ops/s` | `2695500.458 ops/s` | `+38.85%` | `better` |
| `segment-index-lifecycle:openAndCheckAndRepairConsistency` | `276.033 ms/op` | `243.824 ms/op` | `-11.67%` | `worse` |
| `segment-index-lifecycle:openAndCompact` | `298.731 ms/op` | `266.861 ms/op` | `-10.67%` | `worse` |
| `segment-index-lifecycle:openExisting` | `271.967 ms/op` | `240.440 ms/op` | `-11.59%` | `worse` |
| `segment-index-mixed-drain:partitionedIngestMixed` | `541167.334 ops/s` | `524671.310 ops/s` | `-3.05%` | `warning` |
| `segment-index-mixed-drain:partitionedIngestMixed:getWorkload` | `260645.746 ops/s` | `249291.856 ops/s` | `-4.36%` | `warning` |
| `segment-index-mixed-drain:partitionedIngestMixed:putWorkload` | `280521.588 ops/s` | `275379.453 ops/s` | `-1.83%` | `neutral` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed` | `1301123.360 ops/s` | `1271070.192 ops/s` | `-2.31%` | `neutral` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:getWorkload` | `1280341.579 ops/s` | `1250024.625 ops/s` | `-2.37%` | `neutral` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:putWorkload` | `20781.781 ops/s` | `21045.567 ops/s` | `+1.27%` | `neutral` |
| `segment-index-persisted-mutation-concurrent:deleteSync` | `8253.277 ops/s` | `7258.915 ops/s` | `-12.05%` | `worse` |
| `segment-index-persisted-mutation-concurrent:putSync` | `8237.763 ops/s` | `7262.453 ops/s` | `-11.84%` | `worse` |
| `segment-index-persisted-mutation:deleteSync` | `3295.646 ops/s` | `2486.442 ops/s` | `-24.55%` | `worse` |
| `segment-index-persisted-mutation:putSync` | `3172.914 ops/s` | `2428.441 ops/s` | `-23.46%` | `worse` |
| `segment-index-range-scan:boundedScan` | `28.910 us/op` | `29.855 us/op` | `+3.27%` | `better` |
| `segment-index-range-scan:fullStreamRangeFallback` | `2125.392 us/op` | `1998.329 us/op` | `-5.98%` | `warning` |
| `segment-index-range-scan:sequentialRead` | `4027.787 us/op` | `3745.913 us/op` | `-7.00%` | `warning` |
| `segment-merge-sequential:mergeSequential` | `358.379 us/op` | `333.656 us/op` | `-6.90%` | `warning` |
