# Benchmark Comparison

- Profile: `segment-index-nightly`
- Baseline SHA: `6da25f8d2edfcb35fc090cb0bcf177026066b7a3`
- Candidate SHA: `6da25f8d2edfcb35fc090cb0bcf177026066b7a3`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `segment-index-get-live:getHitSync` | `4703065.143 ops/s` | `2780823.058 ops/s` | `-40.87%` | `worse` |
| `segment-index-get-live:getMissSync` | `4284363.694 ops/s` | `2981451.598 ops/s` | `-30.41%` | `worse` |
| `segment-index-get-multisegment-cold:getHitSync` | `281814.923 ops/s` | `312680.425 ops/s` | `+10.95%` | `better` |
| `segment-index-get-multisegment-cold:getMissSync` | `4540365.327 ops/s` | `2459205.515 ops/s` | `-45.84%` | `worse` |
| `segment-index-get-multisegment-hot:getHitSync` | `3705975.215 ops/s` | `2016670.138 ops/s` | `-45.58%` | `worse` |
| `segment-index-get-multisegment-hot:getMissSync` | `4376007.082 ops/s` | `2595476.245 ops/s` | `-40.69%` | `worse` |
| `segment-index-get-persisted:getHitSync` | `3305047.186 ops/s` | `2095148.947 ops/s` | `-36.61%` | `worse` |
| `segment-index-get-persisted:getMissSync` | `4518750.359 ops/s` | `2671817.106 ops/s` | `-40.87%` | `worse` |
| `segment-index-hot-route-put:putHotRoute` | `4585828.696 ops/s` | `2696848.804 ops/s` | `-41.19%` | `worse` |
| `segment-index-hot-route-put:putThenGetHotRoute` | `2081512.301 ops/s` | `1311475.024 ops/s` | `-36.99%` | `worse` |
| `segment-index-lifecycle:openAndCheckAndRepairConsistency` | `244.579 ms/op` | `135.866 ms/op` | `-44.45%` | `worse` |
| `segment-index-lifecycle:openAndCompact` | `259.136 ms/op` | `152.526 ms/op` | `-41.14%` | `worse` |
| `segment-index-lifecycle:openExisting` | `242.755 ms/op` | `133.380 ms/op` | `-45.06%` | `worse` |
| `segment-index-mixed-drain:partitionedIngestMixed` | `557821.022 ops/s` | `492676.341 ops/s` | `-11.68%` | `worse` |
| `segment-index-mixed-drain:partitionedIngestMixed:getWorkload` | `270808.231 ops/s` | `226248.397 ops/s` | `-16.45%` | `worse` |
| `segment-index-mixed-drain:partitionedIngestMixed:putWorkload` | `287012.792 ops/s` | `266427.944 ops/s` | `-7.17%` | `worse` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed` | `1223979.149 ops/s` | `856554.201 ops/s` | `-30.02%` | `worse` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:getWorkload` | `1188135.190 ops/s` | `815299.616 ops/s` | `-31.38%` | `worse` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:putWorkload` | `35843.959 ops/s` | `41254.585 ops/s` | `+15.09%` | `better` |
| `segment-index-persisted-mutation-concurrent:deleteSync` | `8517.506 ops/s` | `5592.737 ops/s` | `-34.34%` | `worse` |
| `segment-index-persisted-mutation-concurrent:putSync` | `8493.047 ops/s` | `5725.347 ops/s` | `-32.59%` | `worse` |
| `segment-index-persisted-mutation:deleteSync` | `3075.287 ops/s` | `2477.065 ops/s` | `-19.45%` | `worse` |
| `segment-index-persisted-mutation:putSync` | `3013.045 ops/s` | `2503.500 ops/s` | `-16.91%` | `worse` |
| `segment-index-range-scan:boundedScan` | `28.291 us/op` | `27.364 us/op` | `-3.28%` | `warning` |
| `segment-index-range-scan:fullStreamRangeFallback` | `1996.766 us/op` | `1800.963 us/op` | `-9.81%` | `worse` |
| `segment-index-range-scan:sequentialRead` | `3919.237 us/op` | `3519.689 us/op` | `-10.19%` | `worse` |
| `segment-merge-sequential:mergeSequential` | `333.057 us/op` | `303.651 us/op` | `-8.83%` | `worse` |
