# Benchmark Comparison

- Profile: `segment-index-nightly`
- Baseline SHA: `d3ef04d30386ff556237923d24a98745e3710b30`
- Candidate SHA: `d3ef04d30386ff556237923d24a98745e3710b30`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `segment-index-get-live:getHitSync` | `4548920.285 ops/s` | `4773372.640 ops/s` | `+4.93%` | `better` |
| `segment-index-get-live:getMissSync` | `4156093.556 ops/s` | `4374633.240 ops/s` | `+5.26%` | `better` |
| `segment-index-get-multisegment-cold:getHitSync` | `285023.108 ops/s` | `274388.392 ops/s` | `-3.73%` | `warning` |
| `segment-index-get-multisegment-cold:getMissSync` | `4475000.164 ops/s` | `4719740.551 ops/s` | `+5.47%` | `better` |
| `segment-index-get-multisegment-hot:getHitSync` | `3713231.315 ops/s` | `3380032.311 ops/s` | `-8.97%` | `worse` |
| `segment-index-get-multisegment-hot:getMissSync` | `4673586.779 ops/s` | `4504597.525 ops/s` | `-3.62%` | `warning` |
| `segment-index-get-persisted:getHitSync` | `3171293.653 ops/s` | `2988822.958 ops/s` | `-5.75%` | `warning` |
| `segment-index-get-persisted:getMissSync` | `4256792.967 ops/s` | `4237793.522 ops/s` | `-0.45%` | `neutral` |
| `segment-index-hot-route-put:putHotRoute` | `4195077.437 ops/s` | `3814135.584 ops/s` | `-9.08%` | `worse` |
| `segment-index-hot-route-put:putThenGetHotRoute` | `2226671.333 ops/s` | `1675490.334 ops/s` | `-24.75%` | `worse` |
| `segment-index-lifecycle:openAndCheckAndRepairConsistency` | `239.992 ms/op` | `279.352 ms/op` | `+16.40%` | `better` |
| `segment-index-lifecycle:openAndCompact` | `261.161 ms/op` | `300.634 ms/op` | `+15.11%` | `better` |
| `segment-index-lifecycle:openExisting` | `237.398 ms/op` | `273.929 ms/op` | `+15.39%` | `better` |
| `segment-index-mixed-drain:partitionedIngestMixed` | `522792.738 ops/s` | `540400.089 ops/s` | `+3.37%` | `better` |
| `segment-index-mixed-drain:partitionedIngestMixed:getWorkload` | `254373.954 ops/s` | `257805.214 ops/s` | `+1.35%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed:putWorkload` | `268418.784 ops/s` | `282594.874 ops/s` | `+5.28%` | `better` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed` | `1236159.909 ops/s` | `1320291.696 ops/s` | `+6.81%` | `better` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:getWorkload` | `1214464.948 ops/s` | `1300406.869 ops/s` | `+7.08%` | `better` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:putWorkload` | `21694.962 ops/s` | `19884.827 ops/s` | `-8.34%` | `worse` |
| `segment-index-persisted-mutation-concurrent:deleteSync` | `7030.232 ops/s` | `7348.541 ops/s` | `+4.53%` | `better` |
| `segment-index-persisted-mutation-concurrent:putSync` | `6817.505 ops/s` | `7368.481 ops/s` | `+8.08%` | `better` |
| `segment-index-persisted-mutation:deleteSync` | `2289.352 ops/s` | `3163.317 ops/s` | `+38.18%` | `better` |
| `segment-index-persisted-mutation:putSync` | `2346.081 ops/s` | `2947.741 ops/s` | `+25.65%` | `better` |
| `segment-index-range-scan:boundedScan` | `30.782 us/op` | `27.998 us/op` | `-9.04%` | `worse` |
| `segment-index-range-scan:fullStreamRangeFallback` | `2007.861 us/op` | `2110.742 us/op` | `+5.12%` | `better` |
| `segment-index-range-scan:sequentialRead` | `3897.893 us/op` | `4209.294 us/op` | `+7.99%` | `better` |
| `segment-merge-sequential:mergeSequential` | `334.279 us/op` | `358.761 us/op` | `+7.32%` | `better` |
