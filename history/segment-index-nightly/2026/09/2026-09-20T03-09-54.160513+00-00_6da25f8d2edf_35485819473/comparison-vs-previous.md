# Benchmark Comparison

- Profile: `segment-index-nightly`
- Baseline SHA: `6da25f8d2edfcb35fc090cb0bcf177026066b7a3`
- Candidate SHA: `6da25f8d2edfcb35fc090cb0bcf177026066b7a3`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `segment-index-get-live:getHitSync` | `4818178.962 ops/s` | `4707494.605 ops/s` | `-2.30%` | `neutral` |
| `segment-index-get-live:getMissSync` | `4507500.530 ops/s` | `4686461.517 ops/s` | `+3.97%` | `better` |
| `segment-index-get-multisegment-cold:getHitSync` | `285000.144 ops/s` | `275339.220 ops/s` | `-3.39%` | `warning` |
| `segment-index-get-multisegment-cold:getMissSync` | `4802291.264 ops/s` | `4568264.591 ops/s` | `-4.87%` | `warning` |
| `segment-index-get-multisegment-hot:getHitSync` | `3813366.795 ops/s` | `3769958.437 ops/s` | `-1.14%` | `neutral` |
| `segment-index-get-multisegment-hot:getMissSync` | `4456845.390 ops/s` | `4309555.205 ops/s` | `-3.30%` | `warning` |
| `segment-index-get-persisted:getHitSync` | `3396190.865 ops/s` | `3458058.488 ops/s` | `+1.82%` | `neutral` |
| `segment-index-get-persisted:getMissSync` | `4757754.978 ops/s` | `4720362.439 ops/s` | `-0.79%` | `neutral` |
| `segment-index-hot-route-put:putHotRoute` | `4405813.587 ops/s` | `4818897.484 ops/s` | `+9.38%` | `better` |
| `segment-index-hot-route-put:putThenGetHotRoute` | `2332192.788 ops/s` | `2352103.363 ops/s` | `+0.85%` | `neutral` |
| `segment-index-lifecycle:openAndCheckAndRepairConsistency` | `244.983 ms/op` | `245.838 ms/op` | `+0.35%` | `neutral` |
| `segment-index-lifecycle:openAndCompact` | `261.492 ms/op` | `259.556 ms/op` | `-0.74%` | `neutral` |
| `segment-index-lifecycle:openExisting` | `242.122 ms/op` | `241.079 ms/op` | `-0.43%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed` | `553380.699 ops/s` | `561525.386 ops/s` | `+1.47%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed:getWorkload` | `261098.804 ops/s` | `265136.208 ops/s` | `+1.55%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed:putWorkload` | `292281.895 ops/s` | `296389.178 ops/s` | `+1.41%` | `neutral` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed` | `1200899.528 ops/s` | `1214295.621 ops/s` | `+1.12%` | `neutral` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:getWorkload` | `1165782.735 ops/s` | `1179653.423 ops/s` | `+1.19%` | `neutral` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:putWorkload` | `35116.793 ops/s` | `34642.198 ops/s` | `-1.35%` | `neutral` |
| `segment-index-persisted-mutation-concurrent:deleteSync` | `6881.026 ops/s` | `3557.119 ops/s` | `-48.31%` | `worse` |
| `segment-index-persisted-mutation-concurrent:putSync` | `6839.123 ops/s` | `3684.198 ops/s` | `-46.13%` | `worse` |
| `segment-index-persisted-mutation:deleteSync` | `2443.344 ops/s` | `1318.363 ops/s` | `-46.04%` | `worse` |
| `segment-index-persisted-mutation:putSync` | `2428.557 ops/s` | `1493.669 ops/s` | `-38.50%` | `worse` |
| `segment-index-range-scan:boundedScan` | `25.829 us/op` | `33.506 us/op` | `+29.72%` | `better` |
| `segment-index-range-scan:fullStreamRangeFallback` | `1998.195 us/op` | `2064.705 us/op` | `+3.33%` | `better` |
| `segment-index-range-scan:sequentialRead` | `4007.920 us/op` | `3840.970 us/op` | `-4.17%` | `warning` |
| `segment-merge-sequential:mergeSequential` | `333.172 us/op` | `331.833 us/op` | `-0.40%` | `neutral` |
