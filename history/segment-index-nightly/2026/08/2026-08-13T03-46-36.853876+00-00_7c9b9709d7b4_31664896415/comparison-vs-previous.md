# Benchmark Comparison

- Profile: `segment-index-nightly`
- Baseline SHA: `7c9b9709d7b4925f77416d396930aabcca6a6987`
- Candidate SHA: `7c9b9709d7b4925f77416d396930aabcca6a6987`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `segment-index-get-live:getHitSync` | `4851639.172 ops/s` | `2511949.297 ops/s` | `-48.22%` | `worse` |
| `segment-index-get-live:getMissSync` | `4977899.984 ops/s` | `2665756.889 ops/s` | `-46.45%` | `worse` |
| `segment-index-get-multisegment-cold:getHitSync` | `273864.490 ops/s` | `331087.830 ops/s` | `+20.89%` | `better` |
| `segment-index-get-multisegment-cold:getMissSync` | `4434252.725 ops/s` | `2667790.232 ops/s` | `-39.84%` | `worse` |
| `segment-index-get-multisegment-hot:getHitSync` | `3573434.641 ops/s` | `2289869.073 ops/s` | `-35.92%` | `worse` |
| `segment-index-get-multisegment-hot:getMissSync` | `4924857.520 ops/s` | `2779268.314 ops/s` | `-43.57%` | `worse` |
| `segment-index-get-persisted:getHitSync` | `3533020.119 ops/s` | `1958839.289 ops/s` | `-44.56%` | `worse` |
| `segment-index-get-persisted:getMissSync` | `5032718.343 ops/s` | `2521108.963 ops/s` | `-49.91%` | `worse` |
| `segment-index-hot-route-put:putHotRoute` | `4374878.573 ops/s` | `3064849.442 ops/s` | `-29.94%` | `worse` |
| `segment-index-hot-route-put:putThenGetHotRoute` | `2358188.273 ops/s` | `1514339.004 ops/s` | `-35.78%` | `worse` |
| `segment-index-lifecycle:openAndCheckAndRepairConsistency` | `242.829 ms/op` | `117.894 ms/op` | `-51.45%` | `worse` |
| `segment-index-lifecycle:openAndCompact` | `265.219 ms/op` | `134.198 ms/op` | `-49.40%` | `worse` |
| `segment-index-lifecycle:openExisting` | `238.304 ms/op` | `113.423 ms/op` | `-52.40%` | `worse` |
| `segment-index-mixed-drain:partitionedIngestMixed` | `526584.784 ops/s` | `515494.525 ops/s` | `-2.11%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed:getWorkload` | `281726.532 ops/s` | `244502.478 ops/s` | `-13.21%` | `worse` |
| `segment-index-mixed-drain:partitionedIngestMixed:putWorkload` | `244858.252 ops/s` | `270992.048 ops/s` | `+10.67%` | `better` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed` | `1188723.288 ops/s` | `1165509.983 ops/s` | `-1.95%` | `neutral` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:getWorkload` | `1174694.524 ops/s` | `1148859.059 ops/s` | `-2.20%` | `neutral` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:putWorkload` | `14028.764 ops/s` | `16650.924 ops/s` | `+18.69%` | `better` |
| `segment-index-persisted-mutation-concurrent:deleteSync` | `6934.370 ops/s` | `1927.689 ops/s` | `-72.20%` | `worse` |
| `segment-index-persisted-mutation-concurrent:putSync` | `6939.033 ops/s` | `1600.849 ops/s` | `-76.93%` | `worse` |
| `segment-index-persisted-mutation:deleteSync` | `2320.608 ops/s` | `1546.967 ops/s` | `-33.34%` | `worse` |
| `segment-index-persisted-mutation:putSync` | `2253.949 ops/s` | `819.618 ops/s` | `-63.64%` | `worse` |
| `segment-index-range-scan:boundedScan` | `27.991 us/op` | `22.083 us/op` | `-21.11%` | `worse` |
| `segment-index-range-scan:fullStreamRangeFallback` | `2042.206 us/op` | `1627.146 us/op` | `-20.32%` | `worse` |
| `segment-index-range-scan:sequentialRead` | `3931.468 us/op` | `3130.370 us/op` | `-20.38%` | `worse` |
| `segment-merge-sequential:mergeSequential` | `331.434 us/op` | `264.326 us/op` | `-20.25%` | `worse` |
