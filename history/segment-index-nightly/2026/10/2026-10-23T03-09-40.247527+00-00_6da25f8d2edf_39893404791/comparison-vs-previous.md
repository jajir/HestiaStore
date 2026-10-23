# Benchmark Comparison

- Profile: `segment-index-nightly`
- Baseline SHA: `6da25f8d2edfcb35fc090cb0bcf177026066b7a3`
- Candidate SHA: `6da25f8d2edfcb35fc090cb0bcf177026066b7a3`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `segment-index-get-live:getHitSync` | `4771343.980 ops/s` | `4703065.143 ops/s` | `-1.43%` | `neutral` |
| `segment-index-get-live:getMissSync` | `5273961.261 ops/s` | `4284363.694 ops/s` | `-18.76%` | `worse` |
| `segment-index-get-multisegment-cold:getHitSync` | `270903.793 ops/s` | `281814.923 ops/s` | `+4.03%` | `better` |
| `segment-index-get-multisegment-cold:getMissSync` | `4592058.744 ops/s` | `4540365.327 ops/s` | `-1.13%` | `neutral` |
| `segment-index-get-multisegment-hot:getHitSync` | `3736897.836 ops/s` | `3705975.215 ops/s` | `-0.83%` | `neutral` |
| `segment-index-get-multisegment-hot:getMissSync` | `4606525.383 ops/s` | `4376007.082 ops/s` | `-5.00%` | `warning` |
| `segment-index-get-persisted:getHitSync` | `3272993.602 ops/s` | `3305047.186 ops/s` | `+0.98%` | `neutral` |
| `segment-index-get-persisted:getMissSync` | `4453359.485 ops/s` | `4518750.359 ops/s` | `+1.47%` | `neutral` |
| `segment-index-hot-route-put:putHotRoute` | `4424330.169 ops/s` | `4585828.696 ops/s` | `+3.65%` | `better` |
| `segment-index-hot-route-put:putThenGetHotRoute` | `2149879.768 ops/s` | `2081512.301 ops/s` | `-3.18%` | `warning` |
| `segment-index-lifecycle:openAndCheckAndRepairConsistency` | `249.081 ms/op` | `244.579 ms/op` | `-1.81%` | `neutral` |
| `segment-index-lifecycle:openAndCompact` | `264.414 ms/op` | `259.136 ms/op` | `-2.00%` | `neutral` |
| `segment-index-lifecycle:openExisting` | `243.610 ms/op` | `242.755 ms/op` | `-0.35%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed` | `559550.052 ops/s` | `557821.022 ops/s` | `-0.31%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed:getWorkload` | `263936.274 ops/s` | `270808.231 ops/s` | `+2.60%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed:putWorkload` | `295613.778 ops/s` | `287012.792 ops/s` | `-2.91%` | `neutral` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed` | `1152236.622 ops/s` | `1223979.149 ops/s` | `+6.23%` | `better` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:getWorkload` | `1116152.201 ops/s` | `1188135.190 ops/s` | `+6.45%` | `better` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:putWorkload` | `36084.421 ops/s` | `35843.959 ops/s` | `-0.67%` | `neutral` |
| `segment-index-persisted-mutation-concurrent:deleteSync` | `8497.612 ops/s` | `8517.506 ops/s` | `+0.23%` | `neutral` |
| `segment-index-persisted-mutation-concurrent:putSync` | `8531.761 ops/s` | `8493.047 ops/s` | `-0.45%` | `neutral` |
| `segment-index-persisted-mutation:deleteSync` | `3264.010 ops/s` | `3075.287 ops/s` | `-5.78%` | `warning` |
| `segment-index-persisted-mutation:putSync` | `3203.903 ops/s` | `3013.045 ops/s` | `-5.96%` | `warning` |
| `segment-index-range-scan:boundedScan` | `27.014 us/op` | `28.291 us/op` | `+4.73%` | `better` |
| `segment-index-range-scan:fullStreamRangeFallback` | `2042.002 us/op` | `1996.766 us/op` | `-2.22%` | `neutral` |
| `segment-index-range-scan:sequentialRead` | `3938.859 us/op` | `3919.237 us/op` | `-0.50%` | `neutral` |
| `segment-merge-sequential:mergeSequential` | `335.301 us/op` | `333.057 us/op` | `-0.67%` | `neutral` |
