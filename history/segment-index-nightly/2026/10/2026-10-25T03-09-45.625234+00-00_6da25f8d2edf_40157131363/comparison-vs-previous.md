# Benchmark Comparison

- Profile: `segment-index-nightly`
- Baseline SHA: `6da25f8d2edfcb35fc090cb0bcf177026066b7a3`
- Candidate SHA: `6da25f8d2edfcb35fc090cb0bcf177026066b7a3`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `segment-index-get-live:getHitSync` | `2780823.058 ops/s` | `6633943.905 ops/s` | `+138.56%` | `better` |
| `segment-index-get-live:getMissSync` | `2981451.598 ops/s` | `5911291.560 ops/s` | `+98.27%` | `better` |
| `segment-index-get-multisegment-cold:getHitSync` | `312680.425 ops/s` | `505335.557 ops/s` | `+61.61%` | `better` |
| `segment-index-get-multisegment-cold:getMissSync` | `2459205.515 ops/s` | `6053958.755 ops/s` | `+146.18%` | `better` |
| `segment-index-get-multisegment-hot:getHitSync` | `2016670.138 ops/s` | `5044043.753 ops/s` | `+150.12%` | `better` |
| `segment-index-get-multisegment-hot:getMissSync` | `2595476.245 ops/s` | `6682645.662 ops/s` | `+157.47%` | `better` |
| `segment-index-get-persisted:getHitSync` | `2095148.947 ops/s` | `5216213.549 ops/s` | `+148.97%` | `better` |
| `segment-index-get-persisted:getMissSync` | `2671817.106 ops/s` | `5946399.807 ops/s` | `+122.56%` | `better` |
| `segment-index-hot-route-put:putHotRoute` | `2696848.804 ops/s` | `5511141.463 ops/s` | `+104.35%` | `better` |
| `segment-index-hot-route-put:putThenGetHotRoute` | `1311475.024 ops/s` | `2999540.487 ops/s` | `+128.72%` | `better` |
| `segment-index-lifecycle:openAndCheckAndRepairConsistency` | `135.866 ms/op` | `218.219 ms/op` | `+60.61%` | `better` |
| `segment-index-lifecycle:openAndCompact` | `152.526 ms/op` | `225.721 ms/op` | `+47.99%` | `better` |
| `segment-index-lifecycle:openExisting` | `133.380 ms/op` | `212.725 ms/op` | `+59.49%` | `better` |
| `segment-index-mixed-drain:partitionedIngestMixed` | `492676.341 ops/s` | `674590.448 ops/s` | `+36.92%` | `better` |
| `segment-index-mixed-drain:partitionedIngestMixed:getWorkload` | `226248.397 ops/s` | `392496.988 ops/s` | `+73.48%` | `better` |
| `segment-index-mixed-drain:partitionedIngestMixed:putWorkload` | `266427.944 ops/s` | `282093.461 ops/s` | `+5.88%` | `better` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed` | `856554.201 ops/s` | `1936556.427 ops/s` | `+126.09%` | `better` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:getWorkload` | `815299.616 ops/s` | `1889265.899 ops/s` | `+131.73%` | `better` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:putWorkload` | `41254.585 ops/s` | `47290.528 ops/s` | `+14.63%` | `better` |
| `segment-index-persisted-mutation-concurrent:deleteSync` | `5592.737 ops/s` | `7875.728 ops/s` | `+40.82%` | `better` |
| `segment-index-persisted-mutation-concurrent:putSync` | `5725.347 ops/s` | `7760.894 ops/s` | `+35.55%` | `better` |
| `segment-index-persisted-mutation:deleteSync` | `2477.065 ops/s` | `3591.626 ops/s` | `+45.00%` | `better` |
| `segment-index-persisted-mutation:putSync` | `2503.500 ops/s` | `3469.387 ops/s` | `+38.58%` | `better` |
| `segment-index-range-scan:boundedScan` | `27.364 us/op` | `21.426 us/op` | `-21.70%` | `worse` |
| `segment-index-range-scan:fullStreamRangeFallback` | `1800.963 us/op` | `1236.962 us/op` | `-31.32%` | `worse` |
| `segment-index-range-scan:sequentialRead` | `3519.689 us/op` | `2390.815 us/op` | `-32.07%` | `worse` |
| `segment-merge-sequential:mergeSequential` | `303.651 us/op` | `186.714 us/op` | `-38.51%` | `worse` |
