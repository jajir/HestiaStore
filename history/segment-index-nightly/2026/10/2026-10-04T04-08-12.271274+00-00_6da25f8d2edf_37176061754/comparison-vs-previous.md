# Benchmark Comparison

- Profile: `segment-index-nightly`
- Baseline SHA: `6da25f8d2edfcb35fc090cb0bcf177026066b7a3`
- Candidate SHA: `6da25f8d2edfcb35fc090cb0bcf177026066b7a3`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `segment-index-get-live:getHitSync` | `4703514.262 ops/s` | `5337862.022 ops/s` | `+13.49%` | `better` |
| `segment-index-get-live:getMissSync` | `4881500.228 ops/s` | `4772371.781 ops/s` | `-2.24%` | `neutral` |
| `segment-index-get-multisegment-cold:getHitSync` | `290195.488 ops/s` | `270582.966 ops/s` | `-6.76%` | `warning` |
| `segment-index-get-multisegment-cold:getMissSync` | `4386202.630 ops/s` | `4719982.760 ops/s` | `+7.61%` | `better` |
| `segment-index-get-multisegment-hot:getHitSync` | `3737684.852 ops/s` | `3616509.006 ops/s` | `-3.24%` | `warning` |
| `segment-index-get-multisegment-hot:getMissSync` | `4473844.035 ops/s` | `5080867.206 ops/s` | `+13.57%` | `better` |
| `segment-index-get-persisted:getHitSync` | `3340685.031 ops/s` | `3358133.426 ops/s` | `+0.52%` | `neutral` |
| `segment-index-get-persisted:getMissSync` | `4603954.398 ops/s` | `4274664.867 ops/s` | `-7.15%` | `worse` |
| `segment-index-hot-route-put:putHotRoute` | `4065279.811 ops/s` | `4352207.202 ops/s` | `+7.06%` | `better` |
| `segment-index-hot-route-put:putThenGetHotRoute` | `2279554.239 ops/s` | `2211468.494 ops/s` | `-2.99%` | `neutral` |
| `segment-index-lifecycle:openAndCheckAndRepairConsistency` | `239.254 ms/op` | `240.406 ms/op` | `+0.48%` | `neutral` |
| `segment-index-lifecycle:openAndCompact` | `257.079 ms/op` | `254.486 ms/op` | `-1.01%` | `neutral` |
| `segment-index-lifecycle:openExisting` | `241.658 ms/op` | `235.989 ms/op` | `-2.35%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed` | `570414.913 ops/s` | `569379.472 ops/s` | `-0.18%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed:getWorkload` | `270340.552 ops/s` | `279100.810 ops/s` | `+3.24%` | `better` |
| `segment-index-mixed-drain:partitionedIngestMixed:putWorkload` | `300074.361 ops/s` | `290278.661 ops/s` | `-3.26%` | `warning` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed` | `1269582.190 ops/s` | `1188386.313 ops/s` | `-6.40%` | `warning` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:getWorkload` | `1233947.863 ops/s` | `1152402.954 ops/s` | `-6.61%` | `warning` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:putWorkload` | `35634.327 ops/s` | `35983.359 ops/s` | `+0.98%` | `neutral` |
| `segment-index-persisted-mutation-concurrent:deleteSync` | `9135.965 ops/s` | `8809.452 ops/s` | `-3.57%` | `warning` |
| `segment-index-persisted-mutation-concurrent:putSync` | `9127.641 ops/s` | `8674.661 ops/s` | `-4.96%` | `warning` |
| `segment-index-persisted-mutation:deleteSync` | `3340.166 ops/s` | `3323.348 ops/s` | `-0.50%` | `neutral` |
| `segment-index-persisted-mutation:putSync` | `3318.895 ops/s` | `3223.394 ops/s` | `-2.88%` | `neutral` |
| `segment-index-range-scan:boundedScan` | `28.348 us/op` | `29.562 us/op` | `+4.28%` | `better` |
| `segment-index-range-scan:fullStreamRangeFallback` | `1969.324 us/op` | `2006.622 us/op` | `+1.89%` | `neutral` |
| `segment-index-range-scan:sequentialRead` | `3880.537 us/op` | `3938.225 us/op` | `+1.49%` | `neutral` |
| `segment-merge-sequential:mergeSequential` | `332.558 us/op` | `336.080 us/op` | `+1.06%` | `neutral` |
