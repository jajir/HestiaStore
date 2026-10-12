# Benchmark Comparison

- Profile: `segment-index-nightly`
- Baseline SHA: `6da25f8d2edfcb35fc090cb0bcf177026066b7a3`
- Candidate SHA: `6da25f8d2edfcb35fc090cb0bcf177026066b7a3`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `segment-index-get-live:getHitSync` | `5303373.493 ops/s` | `4912220.035 ops/s` | `-7.38%` | `worse` |
| `segment-index-get-live:getMissSync` | `4714731.323 ops/s` | `4699267.151 ops/s` | `-0.33%` | `neutral` |
| `segment-index-get-multisegment-cold:getHitSync` | `279606.429 ops/s` | `276329.634 ops/s` | `-1.17%` | `neutral` |
| `segment-index-get-multisegment-cold:getMissSync` | `4201551.238 ops/s` | `4496279.752 ops/s` | `+7.01%` | `better` |
| `segment-index-get-multisegment-hot:getHitSync` | `3576711.689 ops/s` | `3693427.679 ops/s` | `+3.26%` | `better` |
| `segment-index-get-multisegment-hot:getMissSync` | `4520591.449 ops/s` | `4356265.428 ops/s` | `-3.64%` | `warning` |
| `segment-index-get-persisted:getHitSync` | `3070291.988 ops/s` | `3442754.569 ops/s` | `+12.13%` | `better` |
| `segment-index-get-persisted:getMissSync` | `4826178.267 ops/s` | `4525662.032 ops/s` | `-6.23%` | `warning` |
| `segment-index-hot-route-put:putHotRoute` | `4091236.629 ops/s` | `4168275.093 ops/s` | `+1.88%` | `neutral` |
| `segment-index-hot-route-put:putThenGetHotRoute` | `2389424.584 ops/s` | `2312036.056 ops/s` | `-3.24%` | `warning` |
| `segment-index-lifecycle:openAndCheckAndRepairConsistency` | `243.926 ms/op` | `247.511 ms/op` | `+1.47%` | `neutral` |
| `segment-index-lifecycle:openAndCompact` | `259.764 ms/op` | `258.797 ms/op` | `-0.37%` | `neutral` |
| `segment-index-lifecycle:openExisting` | `243.380 ms/op` | `243.492 ms/op` | `+0.05%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed` | `547067.567 ops/s` | `561000.129 ops/s` | `+2.55%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed:getWorkload` | `246815.565 ops/s` | `270173.088 ops/s` | `+9.46%` | `better` |
| `segment-index-mixed-drain:partitionedIngestMixed:putWorkload` | `300252.002 ops/s` | `290827.041 ops/s` | `-3.14%` | `warning` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed` | `1265004.504 ops/s` | `1309562.922 ops/s` | `+3.52%` | `better` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:getWorkload` | `1230539.751 ops/s` | `1275553.099 ops/s` | `+3.66%` | `better` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:putWorkload` | `34464.753 ops/s` | `34009.824 ops/s` | `-1.32%` | `neutral` |
| `segment-index-persisted-mutation-concurrent:deleteSync` | `8836.708 ops/s` | `8934.139 ops/s` | `+1.10%` | `neutral` |
| `segment-index-persisted-mutation-concurrent:putSync` | `8924.734 ops/s` | `8815.809 ops/s` | `-1.22%` | `neutral` |
| `segment-index-persisted-mutation:deleteSync` | `3313.953 ops/s` | `3291.607 ops/s` | `-0.67%` | `neutral` |
| `segment-index-persisted-mutation:putSync` | `3216.739 ops/s` | `3146.004 ops/s` | `-2.20%` | `neutral` |
| `segment-index-range-scan:boundedScan` | `31.776 us/op` | `29.129 us/op` | `-8.33%` | `worse` |
| `segment-index-range-scan:fullStreamRangeFallback` | `1961.361 us/op` | `2015.144 us/op` | `+2.74%` | `neutral` |
| `segment-index-range-scan:sequentialRead` | `3893.790 us/op` | `4025.579 us/op` | `+3.38%` | `better` |
| `segment-merge-sequential:mergeSequential` | `331.487 us/op` | `333.548 us/op` | `+0.62%` | `neutral` |
