# Benchmark Comparison

- Profile: `segment-index-nightly`
- Baseline SHA: `21d1da156bc01e2f39b9f119c5882d79b3da438a`
- Candidate SHA: `dce1fbc911dbad09d8df685697a7c8b43069ed09`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `segment-index-get-live:getHitSync` | `2021551.259 ops/s` | `1911570.828 ops/s` | `-5.44%` | `warning` |
| `segment-index-get-live:getMissSync` | `1968297.066 ops/s` | `1871131.565 ops/s` | `-4.94%` | `warning` |
| `segment-index-get-persisted:getHitSync` | `1557163.616 ops/s` | `1827026.664 ops/s` | `+17.33%` | `better` |
| `segment-index-get-persisted:getMissSync` | `1940017.538 ops/s` | `1908646.128 ops/s` | `-1.62%` | `neutral` |
| `segment-index-hot-route-put:putHotRoute` | `2087470.022 ops/s` | `2041650.221 ops/s` | `-2.19%` | `neutral` |
| `segment-index-hot-route-put:putThenGetHotRoute` | `1017277.452 ops/s` | `1087759.453 ops/s` | `+6.93%` | `better` |
| `segment-index-lifecycle:openAndCheckAndRepairConsistency` | `304.803 ms/op` | `307.237 ms/op` | `+0.80%` | `neutral` |
| `segment-index-lifecycle:openAndCompact` | `327.812 ms/op` | `327.541 ms/op` | `-0.08%` | `neutral` |
| `segment-index-lifecycle:openExisting` | `304.814 ms/op` | `305.467 ms/op` | `+0.21%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed` | `439229.035 ops/s` | `435754.417 ops/s` | `-0.79%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed:getWorkload` | `197007.211 ops/s` | `189366.103 ops/s` | `-3.88%` | `warning` |
| `segment-index-mixed-drain:partitionedIngestMixed:putWorkload` | `242221.824 ops/s` | `246388.314 ops/s` | `+1.72%` | `neutral` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed` | `869829.581 ops/s` | `885715.963 ops/s` | `+1.83%` | `neutral` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:getWorkload` | `851490.256 ops/s` | `867374.349 ops/s` | `+1.87%` | `neutral` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:putWorkload` | `18339.325 ops/s` | `18341.614 ops/s` | `+0.01%` | `neutral` |
