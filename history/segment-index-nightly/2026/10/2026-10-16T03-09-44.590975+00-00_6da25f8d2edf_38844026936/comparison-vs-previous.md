# Benchmark Comparison

- Profile: `segment-index-nightly`
- Baseline SHA: `6da25f8d2edfcb35fc090cb0bcf177026066b7a3`
- Candidate SHA: `6da25f8d2edfcb35fc090cb0bcf177026066b7a3`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `segment-index-get-live:getHitSync` | `6141094.298 ops/s` | `2869795.855 ops/s` | `-53.27%` | `worse` |
| `segment-index-get-live:getMissSync` | `5757195.500 ops/s` | `3130468.559 ops/s` | `-45.63%` | `worse` |
| `segment-index-get-multisegment-cold:getHitSync` | `382306.127 ops/s` | `271138.170 ops/s` | `-29.08%` | `worse` |
| `segment-index-get-multisegment-cold:getMissSync` | `5187088.137 ops/s` | `3129298.828 ops/s` | `-39.67%` | `worse` |
| `segment-index-get-multisegment-hot:getHitSync` | `4577831.037 ops/s` | `2376039.137 ops/s` | `-48.10%` | `worse` |
| `segment-index-get-multisegment-hot:getMissSync` | `6043969.840 ops/s` | `3046569.271 ops/s` | `-49.59%` | `worse` |
| `segment-index-get-persisted:getHitSync` | `4623558.684 ops/s` | `2292089.506 ops/s` | `-50.43%` | `worse` |
| `segment-index-get-persisted:getMissSync` | `5781838.022 ops/s` | `3236654.809 ops/s` | `-44.02%` | `worse` |
| `segment-index-hot-route-put:putHotRoute` | `5264583.364 ops/s` | `2771753.050 ops/s` | `-47.35%` | `worse` |
| `segment-index-hot-route-put:putThenGetHotRoute` | `2528557.760 ops/s` | `1572922.225 ops/s` | `-37.79%` | `worse` |
| `segment-index-lifecycle:openAndCheckAndRepairConsistency` | `214.528 ms/op` | `134.346 ms/op` | `-37.38%` | `worse` |
| `segment-index-lifecycle:openAndCompact` | `228.159 ms/op` | `154.359 ms/op` | `-32.35%` | `worse` |
| `segment-index-lifecycle:openExisting` | `211.500 ms/op` | `132.343 ms/op` | `-37.43%` | `worse` |
| `segment-index-mixed-drain:partitionedIngestMixed` | `666040.665 ops/s` | `460192.976 ops/s` | `-30.91%` | `worse` |
| `segment-index-mixed-drain:partitionedIngestMixed:getWorkload` | `343255.615 ops/s` | `205250.181 ops/s` | `-40.20%` | `worse` |
| `segment-index-mixed-drain:partitionedIngestMixed:putWorkload` | `322785.051 ops/s` | `254942.795 ops/s` | `-21.02%` | `worse` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed` | `1539125.919 ops/s` | `888081.240 ops/s` | `-42.30%` | `worse` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:getWorkload` | `1494526.703 ops/s` | `851264.785 ops/s` | `-43.04%` | `worse` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:putWorkload` | `44599.216 ops/s` | `36816.455 ops/s` | `-17.45%` | `worse` |
| `segment-index-persisted-mutation-concurrent:deleteSync` | `8330.134 ops/s` | `9815.182 ops/s` | `+17.83%` | `better` |
| `segment-index-persisted-mutation-concurrent:putSync` | `6846.042 ops/s` | `9902.948 ops/s` | `+44.65%` | `better` |
| `segment-index-persisted-mutation:deleteSync` | `4018.206 ops/s` | `3948.412 ops/s` | `-1.74%` | `neutral` |
| `segment-index-persisted-mutation:putSync` | `3618.242 ops/s` | `3714.734 ops/s` | `+2.67%` | `neutral` |
| `segment-index-range-scan:boundedScan` | `21.416 us/op` | `28.086 us/op` | `+31.14%` | `better` |
| `segment-index-range-scan:fullStreamRangeFallback` | `1564.676 us/op` | `2024.233 us/op` | `+29.37%` | `better` |
| `segment-index-range-scan:sequentialRead` | `2986.678 us/op` | `3915.174 us/op` | `+31.09%` | `better` |
| `segment-merge-sequential:mergeSequential` | `279.217 us/op` | `333.194 us/op` | `+19.33%` | `better` |
