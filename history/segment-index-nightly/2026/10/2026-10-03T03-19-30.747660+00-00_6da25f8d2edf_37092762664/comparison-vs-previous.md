# Benchmark Comparison

- Profile: `segment-index-nightly`
- Baseline SHA: `6da25f8d2edfcb35fc090cb0bcf177026066b7a3`
- Candidate SHA: `6da25f8d2edfcb35fc090cb0bcf177026066b7a3`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `segment-index-get-live:getHitSync` | `4592102.208 ops/s` | `4703514.262 ops/s` | `+2.43%` | `neutral` |
| `segment-index-get-live:getMissSync` | `5184761.180 ops/s` | `4881500.228 ops/s` | `-5.85%` | `warning` |
| `segment-index-get-multisegment-cold:getHitSync` | `289732.543 ops/s` | `290195.488 ops/s` | `+0.16%` | `neutral` |
| `segment-index-get-multisegment-cold:getMissSync` | `4661423.862 ops/s` | `4386202.630 ops/s` | `-5.90%` | `warning` |
| `segment-index-get-multisegment-hot:getHitSync` | `3726006.574 ops/s` | `3737684.852 ops/s` | `+0.31%` | `neutral` |
| `segment-index-get-multisegment-hot:getMissSync` | `4623272.931 ops/s` | `4473844.035 ops/s` | `-3.23%` | `warning` |
| `segment-index-get-persisted:getHitSync` | `3577451.208 ops/s` | `3340685.031 ops/s` | `-6.62%` | `warning` |
| `segment-index-get-persisted:getMissSync` | `4648064.242 ops/s` | `4603954.398 ops/s` | `-0.95%` | `neutral` |
| `segment-index-hot-route-put:putHotRoute` | `4701392.657 ops/s` | `4065279.811 ops/s` | `-13.53%` | `worse` |
| `segment-index-hot-route-put:putThenGetHotRoute` | `2207084.097 ops/s` | `2279554.239 ops/s` | `+3.28%` | `better` |
| `segment-index-lifecycle:openAndCheckAndRepairConsistency` | `243.182 ms/op` | `239.254 ms/op` | `-1.62%` | `neutral` |
| `segment-index-lifecycle:openAndCompact` | `259.874 ms/op` | `257.079 ms/op` | `-1.08%` | `neutral` |
| `segment-index-lifecycle:openExisting` | `239.758 ms/op` | `241.658 ms/op` | `+0.79%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed` | `560899.151 ops/s` | `570414.913 ops/s` | `+1.70%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed:getWorkload` | `267965.099 ops/s` | `270340.552 ops/s` | `+0.89%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed:putWorkload` | `292934.052 ops/s` | `300074.361 ops/s` | `+2.44%` | `neutral` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed` | `1188560.405 ops/s` | `1269582.190 ops/s` | `+6.82%` | `better` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:getWorkload` | `1154051.555 ops/s` | `1233947.863 ops/s` | `+6.92%` | `better` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:putWorkload` | `34508.850 ops/s` | `35634.327 ops/s` | `+3.26%` | `better` |
| `segment-index-persisted-mutation-concurrent:deleteSync` | `9250.386 ops/s` | `9135.965 ops/s` | `-1.24%` | `neutral` |
| `segment-index-persisted-mutation-concurrent:putSync` | `9287.452 ops/s` | `9127.641 ops/s` | `-1.72%` | `neutral` |
| `segment-index-persisted-mutation:deleteSync` | `3438.298 ops/s` | `3340.166 ops/s` | `-2.85%` | `neutral` |
| `segment-index-persisted-mutation:putSync` | `3339.595 ops/s` | `3318.895 ops/s` | `-0.62%` | `neutral` |
| `segment-index-range-scan:boundedScan` | `30.138 us/op` | `28.348 us/op` | `-5.94%` | `warning` |
| `segment-index-range-scan:fullStreamRangeFallback` | `2005.155 us/op` | `1969.324 us/op` | `-1.79%` | `neutral` |
| `segment-index-range-scan:sequentialRead` | `3847.665 us/op` | `3880.537 us/op` | `+0.85%` | `neutral` |
| `segment-merge-sequential:mergeSequential` | `333.341 us/op` | `332.558 us/op` | `-0.23%` | `neutral` |
