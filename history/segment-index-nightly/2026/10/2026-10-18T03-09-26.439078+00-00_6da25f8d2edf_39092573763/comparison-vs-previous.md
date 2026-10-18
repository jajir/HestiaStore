# Benchmark Comparison

- Profile: `segment-index-nightly`
- Baseline SHA: `6da25f8d2edfcb35fc090cb0bcf177026066b7a3`
- Candidate SHA: `6da25f8d2edfcb35fc090cb0bcf177026066b7a3`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `segment-index-get-live:getHitSync` | `4886642.297 ops/s` | `6846360.469 ops/s` | `+40.10%` | `better` |
| `segment-index-get-live:getMissSync` | `4461053.131 ops/s` | `6694866.771 ops/s` | `+50.07%` | `better` |
| `segment-index-get-multisegment-cold:getHitSync` | `267117.745 ops/s` | `519222.829 ops/s` | `+94.38%` | `better` |
| `segment-index-get-multisegment-cold:getMissSync` | `4560669.699 ops/s` | `6713718.945 ops/s` | `+47.21%` | `better` |
| `segment-index-get-multisegment-hot:getHitSync` | `3761514.882 ops/s` | `5625555.367 ops/s` | `+49.56%` | `better` |
| `segment-index-get-multisegment-hot:getMissSync` | `4622952.189 ops/s` | `6359223.349 ops/s` | `+37.56%` | `better` |
| `segment-index-get-persisted:getHitSync` | `3522015.396 ops/s` | `5065422.024 ops/s` | `+43.82%` | `better` |
| `segment-index-get-persisted:getMissSync` | `4230873.781 ops/s` | `6817734.259 ops/s` | `+61.14%` | `better` |
| `segment-index-hot-route-put:putHotRoute` | `4022091.106 ops/s` | `5837443.296 ops/s` | `+45.13%` | `better` |
| `segment-index-hot-route-put:putThenGetHotRoute` | `2368423.166 ops/s` | `3095268.028 ops/s` | `+30.69%` | `better` |
| `segment-index-lifecycle:openAndCheckAndRepairConsistency` | `243.082 ms/op` | `203.048 ms/op` | `-16.47%` | `worse` |
| `segment-index-lifecycle:openAndCompact` | `260.770 ms/op` | `217.911 ms/op` | `-16.44%` | `worse` |
| `segment-index-lifecycle:openExisting` | `240.347 ms/op` | `204.653 ms/op` | `-14.85%` | `worse` |
| `segment-index-mixed-drain:partitionedIngestMixed` | `571360.708 ops/s` | `723381.676 ops/s` | `+26.61%` | `better` |
| `segment-index-mixed-drain:partitionedIngestMixed:getWorkload` | `279224.133 ops/s` | `437317.395 ops/s` | `+56.62%` | `better` |
| `segment-index-mixed-drain:partitionedIngestMixed:putWorkload` | `292136.576 ops/s` | `286064.280 ops/s` | `-2.08%` | `neutral` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed` | `1199189.298 ops/s` | `1756472.221 ops/s` | `+46.47%` | `better` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:getWorkload` | `1164066.701 ops/s` | `1703403.617 ops/s` | `+46.33%` | `better` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:putWorkload` | `35122.597 ops/s` | `53068.603 ops/s` | `+51.10%` | `better` |
| `segment-index-persisted-mutation-concurrent:deleteSync` | `8516.268 ops/s` | `8213.282 ops/s` | `-3.56%` | `warning` |
| `segment-index-persisted-mutation-concurrent:putSync` | `8683.454 ops/s` | `8828.742 ops/s` | `+1.67%` | `neutral` |
| `segment-index-persisted-mutation:deleteSync` | `3157.252 ops/s` | `3766.719 ops/s` | `+19.30%` | `better` |
| `segment-index-persisted-mutation:putSync` | `3080.169 ops/s` | `3632.908 ops/s` | `+17.95%` | `better` |
| `segment-index-range-scan:boundedScan` | `29.073 us/op` | `17.983 us/op` | `-38.15%` | `worse` |
| `segment-index-range-scan:fullStreamRangeFallback` | `1987.441 us/op` | `1169.184 us/op` | `-41.17%` | `worse` |
| `segment-index-range-scan:sequentialRead` | `3831.888 us/op` | `2286.340 us/op` | `-40.33%` | `worse` |
| `segment-merge-sequential:mergeSequential` | `332.594 us/op` | `181.248 us/op` | `-45.50%` | `worse` |
