# Benchmark Comparison

- Profile: `segment-index-nightly`
- Baseline SHA: `6da25f8d2edfcb35fc090cb0bcf177026066b7a3`
- Candidate SHA: `6da25f8d2edfcb35fc090cb0bcf177026066b7a3`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `segment-index-get-live:getHitSync` | `4707494.605 ops/s` | `4845735.414 ops/s` | `+2.94%` | `neutral` |
| `segment-index-get-live:getMissSync` | `4686461.517 ops/s` | `4513038.503 ops/s` | `-3.70%` | `warning` |
| `segment-index-get-multisegment-cold:getHitSync` | `275339.220 ops/s` | `277108.721 ops/s` | `+0.64%` | `neutral` |
| `segment-index-get-multisegment-cold:getMissSync` | `4568264.591 ops/s` | `4561316.372 ops/s` | `-0.15%` | `neutral` |
| `segment-index-get-multisegment-hot:getHitSync` | `3769958.437 ops/s` | `3604056.359 ops/s` | `-4.40%` | `warning` |
| `segment-index-get-multisegment-hot:getMissSync` | `4309555.205 ops/s` | `4627621.498 ops/s` | `+7.38%` | `better` |
| `segment-index-get-persisted:getHitSync` | `3458058.488 ops/s` | `3594021.202 ops/s` | `+3.93%` | `better` |
| `segment-index-get-persisted:getMissSync` | `4720362.439 ops/s` | `4773420.417 ops/s` | `+1.12%` | `neutral` |
| `segment-index-hot-route-put:putHotRoute` | `4818897.484 ops/s` | `4579821.446 ops/s` | `-4.96%` | `warning` |
| `segment-index-hot-route-put:putThenGetHotRoute` | `2352103.363 ops/s` | `2288535.077 ops/s` | `-2.70%` | `neutral` |
| `segment-index-lifecycle:openAndCheckAndRepairConsistency` | `245.838 ms/op` | `245.526 ms/op` | `-0.13%` | `neutral` |
| `segment-index-lifecycle:openAndCompact` | `259.556 ms/op` | `260.840 ms/op` | `+0.49%` | `neutral` |
| `segment-index-lifecycle:openExisting` | `241.079 ms/op` | `239.211 ms/op` | `-0.78%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed` | `561525.386 ops/s` | `558265.706 ops/s` | `-0.58%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed:getWorkload` | `265136.208 ops/s` | `263269.061 ops/s` | `-0.70%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed:putWorkload` | `296389.178 ops/s` | `294996.644 ops/s` | `-0.47%` | `neutral` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed` | `1214295.621 ops/s` | `1237555.031 ops/s` | `+1.92%` | `neutral` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:getWorkload` | `1179653.423 ops/s` | `1202014.043 ops/s` | `+1.90%` | `neutral` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:putWorkload` | `34642.198 ops/s` | `35540.987 ops/s` | `+2.59%` | `neutral` |
| `segment-index-persisted-mutation-concurrent:deleteSync` | `3557.119 ops/s` | `6985.580 ops/s` | `+96.38%` | `better` |
| `segment-index-persisted-mutation-concurrent:putSync` | `3684.198 ops/s` | `7084.957 ops/s` | `+92.31%` | `better` |
| `segment-index-persisted-mutation:deleteSync` | `1318.363 ops/s` | `2648.800 ops/s` | `+100.92%` | `better` |
| `segment-index-persisted-mutation:putSync` | `1493.669 ops/s` | `2562.466 ops/s` | `+71.56%` | `better` |
| `segment-index-range-scan:boundedScan` | `33.506 us/op` | `28.212 us/op` | `-15.80%` | `worse` |
| `segment-index-range-scan:fullStreamRangeFallback` | `2064.705 us/op` | `2029.261 us/op` | `-1.72%` | `neutral` |
| `segment-index-range-scan:sequentialRead` | `3840.970 us/op` | `3824.373 us/op` | `-0.43%` | `neutral` |
| `segment-merge-sequential:mergeSequential` | `331.833 us/op` | `333.836 us/op` | `+0.60%` | `neutral` |
