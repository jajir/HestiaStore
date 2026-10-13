# Benchmark Comparison

- Profile: `segment-index-nightly`
- Baseline SHA: `6da25f8d2edfcb35fc090cb0bcf177026066b7a3`
- Candidate SHA: `6da25f8d2edfcb35fc090cb0bcf177026066b7a3`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `segment-index-get-live:getHitSync` | `4912220.035 ops/s` | `5229017.109 ops/s` | `+6.45%` | `better` |
| `segment-index-get-live:getMissSync` | `4699267.151 ops/s` | `4273867.324 ops/s` | `-9.05%` | `worse` |
| `segment-index-get-multisegment-cold:getHitSync` | `276329.634 ops/s` | `274204.854 ops/s` | `-0.77%` | `neutral` |
| `segment-index-get-multisegment-cold:getMissSync` | `4496279.752 ops/s` | `4843691.667 ops/s` | `+7.73%` | `better` |
| `segment-index-get-multisegment-hot:getHitSync` | `3693427.679 ops/s` | `3482692.078 ops/s` | `-5.71%` | `warning` |
| `segment-index-get-multisegment-hot:getMissSync` | `4356265.428 ops/s` | `4400023.194 ops/s` | `+1.00%` | `neutral` |
| `segment-index-get-persisted:getHitSync` | `3442754.569 ops/s` | `3385451.409 ops/s` | `-1.66%` | `neutral` |
| `segment-index-get-persisted:getMissSync` | `4525662.032 ops/s` | `4365539.568 ops/s` | `-3.54%` | `warning` |
| `segment-index-hot-route-put:putHotRoute` | `4168275.093 ops/s` | `4502999.033 ops/s` | `+8.03%` | `better` |
| `segment-index-hot-route-put:putThenGetHotRoute` | `2312036.056 ops/s` | `2116285.121 ops/s` | `-8.47%` | `worse` |
| `segment-index-lifecycle:openAndCheckAndRepairConsistency` | `247.511 ms/op` | `245.258 ms/op` | `-0.91%` | `neutral` |
| `segment-index-lifecycle:openAndCompact` | `258.797 ms/op` | `262.307 ms/op` | `+1.36%` | `neutral` |
| `segment-index-lifecycle:openExisting` | `243.492 ms/op` | `242.346 ms/op` | `-0.47%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed` | `561000.129 ops/s` | `550946.627 ops/s` | `-1.79%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed:getWorkload` | `270173.088 ops/s` | `251487.274 ops/s` | `-6.92%` | `warning` |
| `segment-index-mixed-drain:partitionedIngestMixed:putWorkload` | `290827.041 ops/s` | `299459.353 ops/s` | `+2.97%` | `neutral` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed` | `1309562.922 ops/s` | `1215735.771 ops/s` | `-7.16%` | `worse` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:getWorkload` | `1275553.099 ops/s` | `1179226.040 ops/s` | `-7.55%` | `worse` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:putWorkload` | `34009.824 ops/s` | `36509.730 ops/s` | `+7.35%` | `better` |
| `segment-index-persisted-mutation-concurrent:deleteSync` | `8934.139 ops/s` | `7953.093 ops/s` | `-10.98%` | `worse` |
| `segment-index-persisted-mutation-concurrent:putSync` | `8815.809 ops/s` | `8300.425 ops/s` | `-5.85%` | `warning` |
| `segment-index-persisted-mutation:deleteSync` | `3291.607 ops/s` | `3042.575 ops/s` | `-7.57%` | `worse` |
| `segment-index-persisted-mutation:putSync` | `3146.004 ops/s` | `2869.352 ops/s` | `-8.79%` | `worse` |
| `segment-index-range-scan:boundedScan` | `29.129 us/op` | `31.807 us/op` | `+9.19%` | `better` |
| `segment-index-range-scan:fullStreamRangeFallback` | `2015.144 us/op` | `2001.486 us/op` | `-0.68%` | `neutral` |
| `segment-index-range-scan:sequentialRead` | `4025.579 us/op` | `3910.248 us/op` | `-2.86%` | `neutral` |
| `segment-merge-sequential:mergeSequential` | `333.548 us/op` | `334.348 us/op` | `+0.24%` | `neutral` |
