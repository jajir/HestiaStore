# Benchmark Comparison

- Profile: `segment-index-nightly`
- Baseline SHA: `6da25f8d2edfcb35fc090cb0bcf177026066b7a3`
- Candidate SHA: `6da25f8d2edfcb35fc090cb0bcf177026066b7a3`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `segment-index-get-live:getHitSync` | `5769757.064 ops/s` | `4592102.208 ops/s` | `-20.41%` | `worse` |
| `segment-index-get-live:getMissSync` | `5267203.493 ops/s` | `5184761.180 ops/s` | `-1.57%` | `neutral` |
| `segment-index-get-multisegment-cold:getHitSync` | `337089.129 ops/s` | `289732.543 ops/s` | `-14.05%` | `worse` |
| `segment-index-get-multisegment-cold:getMissSync` | `5274814.165 ops/s` | `4661423.862 ops/s` | `-11.63%` | `worse` |
| `segment-index-get-multisegment-hot:getHitSync` | `3567885.962 ops/s` | `3726006.574 ops/s` | `+4.43%` | `better` |
| `segment-index-get-multisegment-hot:getMissSync` | `5403268.404 ops/s` | `4623272.931 ops/s` | `-14.44%` | `worse` |
| `segment-index-get-persisted:getHitSync` | `3776705.941 ops/s` | `3577451.208 ops/s` | `-5.28%` | `warning` |
| `segment-index-get-persisted:getMissSync` | `5031382.830 ops/s` | `4648064.242 ops/s` | `-7.62%` | `worse` |
| `segment-index-hot-route-put:putHotRoute` | `4769706.354 ops/s` | `4701392.657 ops/s` | `-1.43%` | `neutral` |
| `segment-index-hot-route-put:putThenGetHotRoute` | `2407040.344 ops/s` | `2207084.097 ops/s` | `-8.31%` | `worse` |
| `segment-index-lifecycle:openAndCheckAndRepairConsistency` | `226.156 ms/op` | `243.182 ms/op` | `+7.53%` | `better` |
| `segment-index-lifecycle:openAndCompact` | `250.087 ms/op` | `259.874 ms/op` | `+3.91%` | `better` |
| `segment-index-lifecycle:openExisting` | `225.536 ms/op` | `239.758 ms/op` | `+6.31%` | `better` |
| `segment-index-mixed-drain:partitionedIngestMixed` | `635145.619 ops/s` | `560899.151 ops/s` | `-11.69%` | `worse` |
| `segment-index-mixed-drain:partitionedIngestMixed:getWorkload` | `323569.462 ops/s` | `267965.099 ops/s` | `-17.18%` | `worse` |
| `segment-index-mixed-drain:partitionedIngestMixed:putWorkload` | `311576.157 ops/s` | `292934.052 ops/s` | `-5.98%` | `warning` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed` | `1349068.876 ops/s` | `1188560.405 ops/s` | `-11.90%` | `worse` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:getWorkload` | `1306457.912 ops/s` | `1154051.555 ops/s` | `-11.67%` | `worse` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:putWorkload` | `42610.963 ops/s` | `34508.850 ops/s` | `-19.01%` | `worse` |
| `segment-index-persisted-mutation-concurrent:deleteSync` | `5391.501 ops/s` | `9250.386 ops/s` | `+71.57%` | `better` |
| `segment-index-persisted-mutation-concurrent:putSync` | `3214.108 ops/s` | `9287.452 ops/s` | `+188.96%` | `better` |
| `segment-index-persisted-mutation:deleteSync` | `1566.814 ops/s` | `3438.298 ops/s` | `+119.45%` | `better` |
| `segment-index-persisted-mutation:putSync` | `1563.815 ops/s` | `3339.595 ops/s` | `+113.55%` | `better` |
| `segment-index-range-scan:boundedScan` | `23.611 us/op` | `30.138 us/op` | `+27.64%` | `better` |
| `segment-index-range-scan:fullStreamRangeFallback` | `1687.448 us/op` | `2005.155 us/op` | `+18.83%` | `better` |
| `segment-index-range-scan:sequentialRead` | `3360.070 us/op` | `3847.665 us/op` | `+14.51%` | `better` |
| `segment-merge-sequential:mergeSequential` | `307.003 us/op` | `333.341 us/op` | `+8.58%` | `better` |
