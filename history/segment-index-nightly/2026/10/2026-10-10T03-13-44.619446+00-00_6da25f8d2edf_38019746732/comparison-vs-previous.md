# Benchmark Comparison

- Profile: `segment-index-nightly`
- Baseline SHA: `6da25f8d2edfcb35fc090cb0bcf177026066b7a3`
- Candidate SHA: `6da25f8d2edfcb35fc090cb0bcf177026066b7a3`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `segment-index-get-live:getHitSync` | `5124424.918 ops/s` | `5009236.841 ops/s` | `-2.25%` | `neutral` |
| `segment-index-get-live:getMissSync` | `4648467.440 ops/s` | `4369220.747 ops/s` | `-6.01%` | `warning` |
| `segment-index-get-multisegment-cold:getHitSync` | `271955.503 ops/s` | `272622.292 ops/s` | `+0.25%` | `neutral` |
| `segment-index-get-multisegment-cold:getMissSync` | `4477732.817 ops/s` | `4140291.048 ops/s` | `-7.54%` | `worse` |
| `segment-index-get-multisegment-hot:getHitSync` | `3683703.051 ops/s` | `3434518.837 ops/s` | `-6.76%` | `warning` |
| `segment-index-get-multisegment-hot:getMissSync` | `4323613.182 ops/s` | `4536220.134 ops/s` | `+4.92%` | `better` |
| `segment-index-get-persisted:getHitSync` | `3404859.188 ops/s` | `3202812.388 ops/s` | `-5.93%` | `warning` |
| `segment-index-get-persisted:getMissSync` | `4475267.112 ops/s` | `4594093.472 ops/s` | `+2.66%` | `neutral` |
| `segment-index-hot-route-put:putHotRoute` | `4132125.596 ops/s` | `4075737.818 ops/s` | `-1.36%` | `neutral` |
| `segment-index-hot-route-put:putThenGetHotRoute` | `2364291.353 ops/s` | `2222674.097 ops/s` | `-5.99%` | `warning` |
| `segment-index-lifecycle:openAndCheckAndRepairConsistency` | `244.163 ms/op` | `278.922 ms/op` | `+14.24%` | `better` |
| `segment-index-lifecycle:openAndCompact` | `260.686 ms/op` | `291.454 ms/op` | `+11.80%` | `better` |
| `segment-index-lifecycle:openExisting` | `242.674 ms/op` | `272.449 ms/op` | `+12.27%` | `better` |
| `segment-index-mixed-drain:partitionedIngestMixed` | `556237.684 ops/s` | `564617.377 ops/s` | `+1.51%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed:getWorkload` | `263337.398 ops/s` | `272349.280 ops/s` | `+3.42%` | `better` |
| `segment-index-mixed-drain:partitionedIngestMixed:putWorkload` | `292900.286 ops/s` | `292268.097 ops/s` | `-0.22%` | `neutral` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed` | `1182753.807 ops/s` | `1197493.777 ops/s` | `+1.25%` | `neutral` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:getWorkload` | `1146620.218 ops/s` | `1162669.662 ops/s` | `+1.40%` | `neutral` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:putWorkload` | `36133.588 ops/s` | `34824.116 ops/s` | `-3.62%` | `warning` |
| `segment-index-persisted-mutation-concurrent:deleteSync` | `8490.515 ops/s` | `9786.919 ops/s` | `+15.27%` | `better` |
| `segment-index-persisted-mutation-concurrent:putSync` | `8572.081 ops/s` | `10001.986 ops/s` | `+16.68%` | `better` |
| `segment-index-persisted-mutation:deleteSync` | `3193.775 ops/s` | `4259.869 ops/s` | `+33.38%` | `better` |
| `segment-index-persisted-mutation:putSync` | `3064.465 ops/s` | `4146.493 ops/s` | `+35.31%` | `better` |
| `segment-index-range-scan:boundedScan` | `26.831 us/op` | `29.086 us/op` | `+8.41%` | `better` |
| `segment-index-range-scan:fullStreamRangeFallback` | `2082.287 us/op` | `2150.981 us/op` | `+3.30%` | `better` |
| `segment-index-range-scan:sequentialRead` | `3921.514 us/op` | `4072.768 us/op` | `+3.86%` | `better` |
| `segment-merge-sequential:mergeSequential` | `334.034 us/op` | `357.975 us/op` | `+7.17%` | `better` |
