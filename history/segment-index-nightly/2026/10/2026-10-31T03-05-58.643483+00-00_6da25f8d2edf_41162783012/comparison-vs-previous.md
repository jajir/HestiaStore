# Benchmark Comparison

- Profile: `segment-index-nightly`
- Baseline SHA: `6da25f8d2edfcb35fc090cb0bcf177026066b7a3`
- Candidate SHA: `6da25f8d2edfcb35fc090cb0bcf177026066b7a3`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `segment-index-get-live:getHitSync` | `6693322.619 ops/s` | `4888986.305 ops/s` | `-26.96%` | `worse` |
| `segment-index-get-live:getMissSync` | `6775017.762 ops/s` | `4350422.274 ops/s` | `-35.79%` | `worse` |
| `segment-index-get-multisegment-cold:getHitSync` | `491666.569 ops/s` | `279214.294 ops/s` | `-43.21%` | `worse` |
| `segment-index-get-multisegment-cold:getMissSync` | `6417103.693 ops/s` | `4795656.557 ops/s` | `-25.27%` | `worse` |
| `segment-index-get-multisegment-hot:getHitSync` | `5708409.434 ops/s` | `3359333.666 ops/s` | `-41.15%` | `worse` |
| `segment-index-get-multisegment-hot:getMissSync` | `6693228.791 ops/s` | `4349534.114 ops/s` | `-35.02%` | `worse` |
| `segment-index-get-persisted:getHitSync` | `5834099.901 ops/s` | `3329831.745 ops/s` | `-42.92%` | `worse` |
| `segment-index-get-persisted:getMissSync` | `6720773.328 ops/s` | `4729452.925 ops/s` | `-29.63%` | `worse` |
| `segment-index-hot-route-put:putHotRoute` | `6189535.567 ops/s` | `4349728.916 ops/s` | `-29.72%` | `worse` |
| `segment-index-hot-route-put:putThenGetHotRoute` | `3026954.197 ops/s` | `2091812.495 ops/s` | `-30.89%` | `worse` |
| `segment-index-lifecycle:openAndCheckAndRepairConsistency` | `191.114 ms/op` | `227.254 ms/op` | `+18.91%` | `better` |
| `segment-index-lifecycle:openAndCompact` | `198.597 ms/op` | `241.600 ms/op` | `+21.65%` | `better` |
| `segment-index-lifecycle:openExisting` | `185.589 ms/op` | `224.053 ms/op` | `+20.73%` | `better` |
| `segment-index-mixed-drain:partitionedIngestMixed` | `727517.878 ops/s` | `544222.540 ops/s` | `-25.19%` | `worse` |
| `segment-index-mixed-drain:partitionedIngestMixed:getWorkload` | `439641.200 ops/s` | `257287.599 ops/s` | `-41.48%` | `worse` |
| `segment-index-mixed-drain:partitionedIngestMixed:putWorkload` | `287876.678 ops/s` | `286934.940 ops/s` | `-0.33%` | `neutral` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed` | `2008809.990 ops/s` | `1156607.531 ops/s` | `-42.42%` | `worse` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:getWorkload` | `1957980.603 ops/s` | `1120381.845 ops/s` | `-42.78%` | `worse` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:putWorkload` | `50829.387 ops/s` | `36225.685 ops/s` | `-28.73%` | `worse` |
| `segment-index-persisted-mutation-concurrent:deleteSync` | `7954.222 ops/s` | `7785.215 ops/s` | `-2.12%` | `neutral` |
| `segment-index-persisted-mutation-concurrent:putSync` | `7453.503 ops/s` | `7776.039 ops/s` | `+4.33%` | `better` |
| `segment-index-persisted-mutation:deleteSync` | `3835.687 ops/s` | `3518.881 ops/s` | `-8.26%` | `worse` |
| `segment-index-persisted-mutation:putSync` | `3586.610 ops/s` | `3438.708 ops/s` | `-4.12%` | `warning` |
| `segment-index-range-scan:boundedScan` | `18.328 us/op` | `29.471 us/op` | `+60.80%` | `better` |
| `segment-index-range-scan:fullStreamRangeFallback` | `1182.288 us/op` | `2068.342 us/op` | `+74.94%` | `better` |
| `segment-index-range-scan:sequentialRead` | `2262.133 us/op` | `3924.989 us/op` | `+73.51%` | `better` |
| `segment-merge-sequential:mergeSequential` | `181.518 us/op` | `335.439 us/op` | `+84.80%` | `better` |
