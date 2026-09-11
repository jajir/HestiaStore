# Benchmark Comparison

- Profile: `segment-index-nightly`
- Baseline SHA: `d3ef04d30386ff556237923d24a98745e3710b30`
- Candidate SHA: `d3ef04d30386ff556237923d24a98745e3710b30`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `segment-index-get-live:getHitSync` | `4366872.202 ops/s` | `5204219.181 ops/s` | `+19.17%` | `better` |
| `segment-index-get-live:getMissSync` | `4202731.085 ops/s` | `4972128.403 ops/s` | `+18.31%` | `better` |
| `segment-index-get-multisegment-cold:getHitSync` | `285136.899 ops/s` | `270453.775 ops/s` | `-5.15%` | `warning` |
| `segment-index-get-multisegment-cold:getMissSync` | `4447669.352 ops/s` | `4629184.598 ops/s` | `+4.08%` | `better` |
| `segment-index-get-multisegment-hot:getHitSync` | `3424294.128 ops/s` | `3421092.396 ops/s` | `-0.09%` | `neutral` |
| `segment-index-get-multisegment-hot:getMissSync` | `4723682.215 ops/s` | `4318348.558 ops/s` | `-8.58%` | `worse` |
| `segment-index-get-persisted:getHitSync` | `3415291.966 ops/s` | `3539353.089 ops/s` | `+3.63%` | `better` |
| `segment-index-get-persisted:getMissSync` | `4464686.982 ops/s` | `4714659.400 ops/s` | `+5.60%` | `better` |
| `segment-index-hot-route-put:putHotRoute` | `4015341.391 ops/s` | `4466765.947 ops/s` | `+11.24%` | `better` |
| `segment-index-hot-route-put:putThenGetHotRoute` | `2149260.887 ops/s` | `2047993.403 ops/s` | `-4.71%` | `warning` |
| `segment-index-lifecycle:openAndCheckAndRepairConsistency` | `274.939 ms/op` | `244.607 ms/op` | `-11.03%` | `worse` |
| `segment-index-lifecycle:openAndCompact` | `299.114 ms/op` | `270.380 ms/op` | `-9.61%` | `worse` |
| `segment-index-lifecycle:openExisting` | `271.393 ms/op` | `241.292 ms/op` | `-11.09%` | `worse` |
| `segment-index-mixed-drain:partitionedIngestMixed` | `566580.783 ops/s` | `550257.271 ops/s` | `-2.88%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed:getWorkload` | `280750.246 ops/s` | `276026.728 ops/s` | `-1.68%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed:putWorkload` | `285830.537 ops/s` | `274230.543 ops/s` | `-4.06%` | `warning` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed` | `1312383.582 ops/s` | `1259679.908 ops/s` | `-4.02%` | `warning` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:getWorkload` | `1291473.739 ops/s` | `1239196.548 ops/s` | `-4.05%` | `warning` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:putWorkload` | `20909.843 ops/s` | `20483.360 ops/s` | `-2.04%` | `neutral` |
| `segment-index-persisted-mutation-concurrent:deleteSync` | `7888.205 ops/s` | `6664.129 ops/s` | `-15.52%` | `worse` |
| `segment-index-persisted-mutation-concurrent:putSync` | `7791.823 ops/s` | `6645.344 ops/s` | `-14.71%` | `worse` |
| `segment-index-persisted-mutation:deleteSync` | `3150.101 ops/s` | `2241.705 ops/s` | `-28.84%` | `worse` |
| `segment-index-persisted-mutation:putSync` | `3077.620 ops/s` | `2275.853 ops/s` | `-26.05%` | `worse` |
| `segment-index-range-scan:boundedScan` | `29.817 us/op` | `29.748 us/op` | `-0.23%` | `neutral` |
| `segment-index-range-scan:fullStreamRangeFallback` | `2108.303 us/op` | `1992.369 us/op` | `-5.50%` | `warning` |
| `segment-index-range-scan:sequentialRead` | `4050.324 us/op` | `3914.293 us/op` | `-3.36%` | `warning` |
| `segment-merge-sequential:mergeSequential` | `359.489 us/op` | `335.408 us/op` | `-6.70%` | `warning` |
