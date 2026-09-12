# Benchmark Comparison

- Profile: `segment-index-nightly`
- Baseline SHA: `d3ef04d30386ff556237923d24a98745e3710b30`
- Candidate SHA: `d3ef04d30386ff556237923d24a98745e3710b30`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `segment-index-get-live:getHitSync` | `5204219.181 ops/s` | `4775434.661 ops/s` | `-8.24%` | `worse` |
| `segment-index-get-live:getMissSync` | `4972128.403 ops/s` | `4659808.604 ops/s` | `-6.28%` | `warning` |
| `segment-index-get-multisegment-cold:getHitSync` | `270453.775 ops/s` | `272155.497 ops/s` | `+0.63%` | `neutral` |
| `segment-index-get-multisegment-cold:getMissSync` | `4629184.598 ops/s` | `4037468.893 ops/s` | `-12.78%` | `worse` |
| `segment-index-get-multisegment-hot:getHitSync` | `3421092.396 ops/s` | `3766404.923 ops/s` | `+10.09%` | `better` |
| `segment-index-get-multisegment-hot:getMissSync` | `4318348.558 ops/s` | `4112741.370 ops/s` | `-4.76%` | `warning` |
| `segment-index-get-persisted:getHitSync` | `3539353.089 ops/s` | `3558684.236 ops/s` | `+0.55%` | `neutral` |
| `segment-index-get-persisted:getMissSync` | `4714659.400 ops/s` | `4705293.479 ops/s` | `-0.20%` | `neutral` |
| `segment-index-hot-route-put:putHotRoute` | `4466765.947 ops/s` | `4277453.342 ops/s` | `-4.24%` | `warning` |
| `segment-index-hot-route-put:putThenGetHotRoute` | `2047993.403 ops/s` | `2177668.247 ops/s` | `+6.33%` | `better` |
| `segment-index-lifecycle:openAndCheckAndRepairConsistency` | `244.607 ms/op` | `278.856 ms/op` | `+14.00%` | `better` |
| `segment-index-lifecycle:openAndCompact` | `270.380 ms/op` | `301.572 ms/op` | `+11.54%` | `better` |
| `segment-index-lifecycle:openExisting` | `241.292 ms/op` | `274.295 ms/op` | `+13.68%` | `better` |
| `segment-index-mixed-drain:partitionedIngestMixed` | `550257.271 ops/s` | `558967.981 ops/s` | `+1.58%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed:getWorkload` | `276026.728 ops/s` | `274630.572 ops/s` | `-0.51%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed:putWorkload` | `274230.543 ops/s` | `284337.409 ops/s` | `+3.69%` | `better` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed` | `1259679.908 ops/s` | `1286324.330 ops/s` | `+2.12%` | `neutral` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:getWorkload` | `1239196.548 ops/s` | `1266789.090 ops/s` | `+2.23%` | `neutral` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:putWorkload` | `20483.360 ops/s` | `19535.239 ops/s` | `-4.63%` | `warning` |
| `segment-index-persisted-mutation-concurrent:deleteSync` | `6664.129 ops/s` | `8161.992 ops/s` | `+22.48%` | `better` |
| `segment-index-persisted-mutation-concurrent:putSync` | `6645.344 ops/s` | `8114.696 ops/s` | `+22.11%` | `better` |
| `segment-index-persisted-mutation:deleteSync` | `2241.705 ops/s` | `3281.184 ops/s` | `+46.37%` | `better` |
| `segment-index-persisted-mutation:putSync` | `2275.853 ops/s` | `3183.280 ops/s` | `+39.87%` | `better` |
| `segment-index-range-scan:boundedScan` | `29.748 us/op` | `32.046 us/op` | `+7.72%` | `better` |
| `segment-index-range-scan:fullStreamRangeFallback` | `1992.369 us/op` | `2119.844 us/op` | `+6.40%` | `better` |
| `segment-index-range-scan:sequentialRead` | `3914.293 us/op` | `4147.615 us/op` | `+5.96%` | `better` |
| `segment-merge-sequential:mergeSequential` | `335.408 us/op` | `358.482 us/op` | `+6.88%` | `better` |
