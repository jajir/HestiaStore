# Benchmark Comparison

- Profile: `segment-index-nightly`
- Baseline SHA: `4c62ce188ffb07fdedd6c5d5d57d0453a33563a3`
- Candidate SHA: `99f6033423f943fa22655f00429ed039230292dc`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `segment-index-get-live:getHitSync` | `5466910.675 ops/s` | `5283369.640 ops/s` | `-3.36%` | `warning` |
| `segment-index-get-live:getMissSync` | `4539613.379 ops/s` | `4342179.377 ops/s` | `-4.35%` | `warning` |
| `segment-index-get-multisegment-cold:getHitSync` | `271278.214 ops/s` | `277972.193 ops/s` | `+2.47%` | `neutral` |
| `segment-index-get-multisegment-cold:getMissSync` | `4409311.354 ops/s` | `4749656.009 ops/s` | `+7.72%` | `better` |
| `segment-index-get-multisegment-hot:getHitSync` | `3547613.462 ops/s` | `3584209.518 ops/s` | `+1.03%` | `neutral` |
| `segment-index-get-multisegment-hot:getMissSync` | `4760429.615 ops/s` | `4635316.033 ops/s` | `-2.63%` | `neutral` |
| `segment-index-get-persisted:getHitSync` | `3614490.274 ops/s` | `3813667.923 ops/s` | `+5.51%` | `better` |
| `segment-index-get-persisted:getMissSync` | `4482827.664 ops/s` | `5153795.832 ops/s` | `+14.97%` | `better` |
| `segment-index-hot-route-put:putHotRoute` | `4472668.504 ops/s` | `4274847.289 ops/s` | `-4.42%` | `warning` |
| `segment-index-hot-route-put:putThenGetHotRoute` | `2358030.585 ops/s` | `2348899.400 ops/s` | `-0.39%` | `neutral` |
| `segment-index-lifecycle:openAndCheckAndRepairConsistency` | `254.223 ms/op` | `256.158 ms/op` | `+0.76%` | `neutral` |
| `segment-index-lifecycle:openAndCompact` | `278.152 ms/op` | `279.320 ms/op` | `+0.42%` | `neutral` |
| `segment-index-lifecycle:openExisting` | `253.143 ms/op` | `252.226 ms/op` | `-0.36%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed` | `529631.242 ops/s` | `519361.234 ops/s` | `-1.94%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed:getWorkload` | `283840.702 ops/s` | `271956.777 ops/s` | `-4.19%` | `warning` |
| `segment-index-mixed-drain:partitionedIngestMixed:putWorkload` | `245790.539 ops/s` | `247404.457 ops/s` | `+0.66%` | `neutral` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed` | `1133395.825 ops/s` | `1196571.576 ops/s` | `+5.57%` | `better` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:getWorkload` | `1118822.179 ops/s` | `1181289.901 ops/s` | `+5.58%` | `better` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:putWorkload` | `14573.647 ops/s` | `15281.675 ops/s` | `+4.86%` | `better` |
| `segment-index-persisted-mutation-concurrent:deleteSync` | `6945.553 ops/s` | `6723.070 ops/s` | `-3.20%` | `warning` |
| `segment-index-persisted-mutation-concurrent:putSync` | `7075.453 ops/s` | `6824.380 ops/s` | `-3.55%` | `warning` |
| `segment-index-persisted-mutation:deleteSync` | `2346.085 ops/s` | `2387.474 ops/s` | `+1.76%` | `neutral` |
| `segment-index-persisted-mutation:putSync` | `2309.863 ops/s` | `2238.750 ops/s` | `-3.08%` | `warning` |
| `segment-index-range-scan:boundedScan` | `29.469 us/op` | `31.401 us/op` | `+6.56%` | `better` |
| `segment-index-range-scan:fullStreamRangeFallback` | `2083.896 us/op` | `2112.223 us/op` | `+1.36%` | `neutral` |
| `segment-index-range-scan:sequentialRead` | `4088.422 us/op` | `4039.157 us/op` | `-1.20%` | `neutral` |
| `segment-merge-sequential:mergeSequential` | `333.064 us/op` | `332.950 us/op` | `-0.03%` | `neutral` |
