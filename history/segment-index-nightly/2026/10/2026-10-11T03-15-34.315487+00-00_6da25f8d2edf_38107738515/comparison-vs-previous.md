# Benchmark Comparison

- Profile: `segment-index-nightly`
- Baseline SHA: `6da25f8d2edfcb35fc090cb0bcf177026066b7a3`
- Candidate SHA: `6da25f8d2edfcb35fc090cb0bcf177026066b7a3`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `segment-index-get-live:getHitSync` | `5009236.841 ops/s` | `5303373.493 ops/s` | `+5.87%` | `better` |
| `segment-index-get-live:getMissSync` | `4369220.747 ops/s` | `4714731.323 ops/s` | `+7.91%` | `better` |
| `segment-index-get-multisegment-cold:getHitSync` | `272622.292 ops/s` | `279606.429 ops/s` | `+2.56%` | `neutral` |
| `segment-index-get-multisegment-cold:getMissSync` | `4140291.048 ops/s` | `4201551.238 ops/s` | `+1.48%` | `neutral` |
| `segment-index-get-multisegment-hot:getHitSync` | `3434518.837 ops/s` | `3576711.689 ops/s` | `+4.14%` | `better` |
| `segment-index-get-multisegment-hot:getMissSync` | `4536220.134 ops/s` | `4520591.449 ops/s` | `-0.34%` | `neutral` |
| `segment-index-get-persisted:getHitSync` | `3202812.388 ops/s` | `3070291.988 ops/s` | `-4.14%` | `warning` |
| `segment-index-get-persisted:getMissSync` | `4594093.472 ops/s` | `4826178.267 ops/s` | `+5.05%` | `better` |
| `segment-index-hot-route-put:putHotRoute` | `4075737.818 ops/s` | `4091236.629 ops/s` | `+0.38%` | `neutral` |
| `segment-index-hot-route-put:putThenGetHotRoute` | `2222674.097 ops/s` | `2389424.584 ops/s` | `+7.50%` | `better` |
| `segment-index-lifecycle:openAndCheckAndRepairConsistency` | `278.922 ms/op` | `243.926 ms/op` | `-12.55%` | `worse` |
| `segment-index-lifecycle:openAndCompact` | `291.454 ms/op` | `259.764 ms/op` | `-10.87%` | `worse` |
| `segment-index-lifecycle:openExisting` | `272.449 ms/op` | `243.380 ms/op` | `-10.67%` | `worse` |
| `segment-index-mixed-drain:partitionedIngestMixed` | `564617.377 ops/s` | `547067.567 ops/s` | `-3.11%` | `warning` |
| `segment-index-mixed-drain:partitionedIngestMixed:getWorkload` | `272349.280 ops/s` | `246815.565 ops/s` | `-9.38%` | `worse` |
| `segment-index-mixed-drain:partitionedIngestMixed:putWorkload` | `292268.097 ops/s` | `300252.002 ops/s` | `+2.73%` | `neutral` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed` | `1197493.777 ops/s` | `1265004.504 ops/s` | `+5.64%` | `better` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:getWorkload` | `1162669.662 ops/s` | `1230539.751 ops/s` | `+5.84%` | `better` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:putWorkload` | `34824.116 ops/s` | `34464.753 ops/s` | `-1.03%` | `neutral` |
| `segment-index-persisted-mutation-concurrent:deleteSync` | `9786.919 ops/s` | `8836.708 ops/s` | `-9.71%` | `worse` |
| `segment-index-persisted-mutation-concurrent:putSync` | `10001.986 ops/s` | `8924.734 ops/s` | `-10.77%` | `worse` |
| `segment-index-persisted-mutation:deleteSync` | `4259.869 ops/s` | `3313.953 ops/s` | `-22.21%` | `worse` |
| `segment-index-persisted-mutation:putSync` | `4146.493 ops/s` | `3216.739 ops/s` | `-22.42%` | `worse` |
| `segment-index-range-scan:boundedScan` | `29.086 us/op` | `31.776 us/op` | `+9.25%` | `better` |
| `segment-index-range-scan:fullStreamRangeFallback` | `2150.981 us/op` | `1961.361 us/op` | `-8.82%` | `worse` |
| `segment-index-range-scan:sequentialRead` | `4072.768 us/op` | `3893.790 us/op` | `-4.39%` | `warning` |
| `segment-merge-sequential:mergeSequential` | `357.975 us/op` | `331.487 us/op` | `-7.40%` | `worse` |
