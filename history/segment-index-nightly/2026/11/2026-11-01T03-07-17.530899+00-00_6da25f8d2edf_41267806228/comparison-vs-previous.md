# Benchmark Comparison

- Profile: `segment-index-nightly`
- Baseline SHA: `6da25f8d2edfcb35fc090cb0bcf177026066b7a3`
- Candidate SHA: `6da25f8d2edfcb35fc090cb0bcf177026066b7a3`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `segment-index-get-live:getHitSync` | `4888986.305 ops/s` | `3221079.754 ops/s` | `-34.12%` | `worse` |
| `segment-index-get-live:getMissSync` | `4350422.274 ops/s` | `2372387.800 ops/s` | `-45.47%` | `worse` |
| `segment-index-get-multisegment-cold:getHitSync` | `279214.294 ops/s` | `317962.206 ops/s` | `+13.88%` | `better` |
| `segment-index-get-multisegment-cold:getMissSync` | `4795656.557 ops/s` | `2382232.397 ops/s` | `-50.33%` | `worse` |
| `segment-index-get-multisegment-hot:getHitSync` | `3359333.666 ops/s` | `1991537.459 ops/s` | `-40.72%` | `worse` |
| `segment-index-get-multisegment-hot:getMissSync` | `4349534.114 ops/s` | `2976488.199 ops/s` | `-31.57%` | `worse` |
| `segment-index-get-persisted:getHitSync` | `3329831.745 ops/s` | `2075155.474 ops/s` | `-37.68%` | `worse` |
| `segment-index-get-persisted:getMissSync` | `4729452.925 ops/s` | `2928479.453 ops/s` | `-38.08%` | `worse` |
| `segment-index-hot-route-put:putHotRoute` | `4349728.916 ops/s` | `2613336.238 ops/s` | `-39.92%` | `worse` |
| `segment-index-hot-route-put:putThenGetHotRoute` | `2091812.495 ops/s` | `1354229.113 ops/s` | `-35.26%` | `worse` |
| `segment-index-lifecycle:openAndCheckAndRepairConsistency` | `227.254 ms/op` | `110.935 ms/op` | `-51.18%` | `worse` |
| `segment-index-lifecycle:openAndCompact` | `241.600 ms/op` | `125.567 ms/op` | `-48.03%` | `worse` |
| `segment-index-lifecycle:openExisting` | `224.053 ms/op` | `106.369 ms/op` | `-52.53%` | `worse` |
| `segment-index-mixed-drain:partitionedIngestMixed` | `544222.540 ops/s` | `512821.092 ops/s` | `-5.77%` | `warning` |
| `segment-index-mixed-drain:partitionedIngestMixed:getWorkload` | `257287.599 ops/s` | `241088.552 ops/s` | `-6.30%` | `warning` |
| `segment-index-mixed-drain:partitionedIngestMixed:putWorkload` | `286934.940 ops/s` | `271732.540 ops/s` | `-5.30%` | `warning` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed` | `1156607.531 ops/s` | `899354.301 ops/s` | `-22.24%` | `worse` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:getWorkload` | `1120381.845 ops/s` | `858591.152 ops/s` | `-23.37%` | `worse` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:putWorkload` | `36225.685 ops/s` | `40763.149 ops/s` | `+12.53%` | `better` |
| `segment-index-persisted-mutation-concurrent:deleteSync` | `7785.215 ops/s` | `6347.061 ops/s` | `-18.47%` | `worse` |
| `segment-index-persisted-mutation-concurrent:putSync` | `7776.039 ops/s` | `6058.905 ops/s` | `-22.08%` | `worse` |
| `segment-index-persisted-mutation:deleteSync` | `3518.881 ops/s` | `2803.198 ops/s` | `-20.34%` | `worse` |
| `segment-index-persisted-mutation:putSync` | `3438.708 ops/s` | `2751.106 ops/s` | `-20.00%` | `worse` |
| `segment-index-range-scan:boundedScan` | `29.471 us/op` | `21.413 us/op` | `-27.34%` | `worse` |
| `segment-index-range-scan:fullStreamRangeFallback` | `2068.342 us/op` | `1728.798 us/op` | `-16.42%` | `worse` |
| `segment-index-range-scan:sequentialRead` | `3924.989 us/op` | `3245.708 us/op` | `-17.31%` | `worse` |
| `segment-merge-sequential:mergeSequential` | `335.439 us/op` | `274.445 us/op` | `-18.18%` | `worse` |
