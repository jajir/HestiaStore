# Benchmark Comparison

- Profile: `segment-index-nightly`
- Baseline SHA: `6da25f8d2edfcb35fc090cb0bcf177026066b7a3`
- Candidate SHA: `6da25f8d2edfcb35fc090cb0bcf177026066b7a3`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `segment-index-get-live:getHitSync` | `5428696.470 ops/s` | `4736040.605 ops/s` | `-12.76%` | `worse` |
| `segment-index-get-live:getMissSync` | `4786638.905 ops/s` | `4746472.566 ops/s` | `-0.84%` | `neutral` |
| `segment-index-get-multisegment-cold:getHitSync` | `287764.602 ops/s` | `273389.728 ops/s` | `-5.00%` | `warning` |
| `segment-index-get-multisegment-cold:getMissSync` | `4826750.249 ops/s` | `4743346.769 ops/s` | `-1.73%` | `neutral` |
| `segment-index-get-multisegment-hot:getHitSync` | `3613440.833 ops/s` | `3719666.311 ops/s` | `+2.94%` | `neutral` |
| `segment-index-get-multisegment-hot:getMissSync` | `4404949.677 ops/s` | `4281136.297 ops/s` | `-2.81%` | `neutral` |
| `segment-index-get-persisted:getHitSync` | `3088759.492 ops/s` | `3397019.714 ops/s` | `+9.98%` | `better` |
| `segment-index-get-persisted:getMissSync` | `4693640.719 ops/s` | `4654464.615 ops/s` | `-0.83%` | `neutral` |
| `segment-index-hot-route-put:putHotRoute` | `4322308.566 ops/s` | `4259321.561 ops/s` | `-1.46%` | `neutral` |
| `segment-index-hot-route-put:putThenGetHotRoute` | `2368640.149 ops/s` | `2242340.177 ops/s` | `-5.33%` | `warning` |
| `segment-index-lifecycle:openAndCheckAndRepairConsistency` | `239.569 ms/op` | `247.452 ms/op` | `+3.29%` | `better` |
| `segment-index-lifecycle:openAndCompact` | `257.090 ms/op` | `259.475 ms/op` | `+0.93%` | `neutral` |
| `segment-index-lifecycle:openExisting` | `238.002 ms/op` | `240.230 ms/op` | `+0.94%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed` | `565781.340 ops/s` | `570675.885 ops/s` | `+0.87%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed:getWorkload` | `278358.814 ops/s` | `278919.883 ops/s` | `+0.20%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed:putWorkload` | `287422.526 ops/s` | `291756.002 ops/s` | `+1.51%` | `neutral` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed` | `1189909.765 ops/s` | `1223874.217 ops/s` | `+2.85%` | `neutral` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:getWorkload` | `1152949.601 ops/s` | `1188297.555 ops/s` | `+3.07%` | `better` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:putWorkload` | `36960.164 ops/s` | `35576.662 ops/s` | `-3.74%` | `warning` |
| `segment-index-persisted-mutation-concurrent:deleteSync` | `7904.954 ops/s` | `6230.169 ops/s` | `-21.19%` | `worse` |
| `segment-index-persisted-mutation-concurrent:putSync` | `8024.851 ops/s` | `5227.048 ops/s` | `-34.86%` | `worse` |
| `segment-index-persisted-mutation:deleteSync` | `2826.336 ops/s` | `1963.527 ops/s` | `-30.53%` | `worse` |
| `segment-index-persisted-mutation:putSync` | `2821.409 ops/s` | `2231.220 ops/s` | `-20.92%` | `worse` |
| `segment-index-range-scan:boundedScan` | `26.325 us/op` | `32.262 us/op` | `+22.55%` | `better` |
| `segment-index-range-scan:fullStreamRangeFallback` | `1985.544 us/op` | `1987.824 us/op` | `+0.11%` | `neutral` |
| `segment-index-range-scan:sequentialRead` | `3864.164 us/op` | `3883.450 us/op` | `+0.50%` | `neutral` |
| `segment-merge-sequential:mergeSequential` | `335.221 us/op` | `335.076 us/op` | `-0.04%` | `neutral` |
