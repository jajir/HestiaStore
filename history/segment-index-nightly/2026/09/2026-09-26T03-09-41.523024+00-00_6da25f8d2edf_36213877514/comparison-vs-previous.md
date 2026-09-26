# Benchmark Comparison

- Profile: `segment-index-nightly`
- Baseline SHA: `6da25f8d2edfcb35fc090cb0bcf177026066b7a3`
- Candidate SHA: `6da25f8d2edfcb35fc090cb0bcf177026066b7a3`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `segment-index-get-live:getHitSync` | `5004691.386 ops/s` | `5428696.470 ops/s` | `+8.47%` | `better` |
| `segment-index-get-live:getMissSync` | `4743860.567 ops/s` | `4786638.905 ops/s` | `+0.90%` | `neutral` |
| `segment-index-get-multisegment-cold:getHitSync` | `277950.145 ops/s` | `287764.602 ops/s` | `+3.53%` | `better` |
| `segment-index-get-multisegment-cold:getMissSync` | `4720233.967 ops/s` | `4826750.249 ops/s` | `+2.26%` | `neutral` |
| `segment-index-get-multisegment-hot:getHitSync` | `3745333.867 ops/s` | `3613440.833 ops/s` | `-3.52%` | `warning` |
| `segment-index-get-multisegment-hot:getMissSync` | `5600982.082 ops/s` | `4404949.677 ops/s` | `-21.35%` | `worse` |
| `segment-index-get-persisted:getHitSync` | `3522901.736 ops/s` | `3088759.492 ops/s` | `-12.32%` | `worse` |
| `segment-index-get-persisted:getMissSync` | `5082734.243 ops/s` | `4693640.719 ops/s` | `-7.66%` | `worse` |
| `segment-index-hot-route-put:putHotRoute` | `4552427.792 ops/s` | `4322308.566 ops/s` | `-5.05%` | `warning` |
| `segment-index-hot-route-put:putThenGetHotRoute` | `2394172.601 ops/s` | `2368640.149 ops/s` | `-1.07%` | `neutral` |
| `segment-index-lifecycle:openAndCheckAndRepairConsistency` | `245.080 ms/op` | `239.569 ms/op` | `-2.25%` | `neutral` |
| `segment-index-lifecycle:openAndCompact` | `262.230 ms/op` | `257.090 ms/op` | `-1.96%` | `neutral` |
| `segment-index-lifecycle:openExisting` | `240.296 ms/op` | `238.002 ms/op` | `-0.95%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed` | `557358.912 ops/s` | `565781.340 ops/s` | `+1.51%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed:getWorkload` | `264693.487 ops/s` | `278358.814 ops/s` | `+5.16%` | `better` |
| `segment-index-mixed-drain:partitionedIngestMixed:putWorkload` | `292665.425 ops/s` | `287422.526 ops/s` | `-1.79%` | `neutral` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed` | `1243703.263 ops/s` | `1189909.765 ops/s` | `-4.33%` | `warning` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:getWorkload` | `1208755.142 ops/s` | `1152949.601 ops/s` | `-4.62%` | `warning` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:putWorkload` | `34948.121 ops/s` | `36960.164 ops/s` | `+5.76%` | `better` |
| `segment-index-persisted-mutation-concurrent:deleteSync` | `8749.071 ops/s` | `7904.954 ops/s` | `-9.65%` | `worse` |
| `segment-index-persisted-mutation-concurrent:putSync` | `8534.897 ops/s` | `8024.851 ops/s` | `-5.98%` | `warning` |
| `segment-index-persisted-mutation:deleteSync` | `3158.270 ops/s` | `2826.336 ops/s` | `-10.51%` | `worse` |
| `segment-index-persisted-mutation:putSync` | `3108.922 ops/s` | `2821.409 ops/s` | `-9.25%` | `worse` |
| `segment-index-range-scan:boundedScan` | `28.522 us/op` | `26.325 us/op` | `-7.70%` | `worse` |
| `segment-index-range-scan:fullStreamRangeFallback` | `1972.901 us/op` | `1985.544 us/op` | `+0.64%` | `neutral` |
| `segment-index-range-scan:sequentialRead` | `3827.985 us/op` | `3864.164 us/op` | `+0.95%` | `neutral` |
| `segment-merge-sequential:mergeSequential` | `331.085 us/op` | `335.221 us/op` | `+1.25%` | `neutral` |
