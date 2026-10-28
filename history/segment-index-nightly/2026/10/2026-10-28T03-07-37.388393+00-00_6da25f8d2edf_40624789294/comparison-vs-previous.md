# Benchmark Comparison

- Profile: `segment-index-nightly`
- Baseline SHA: `6da25f8d2edfcb35fc090cb0bcf177026066b7a3`
- Candidate SHA: `6da25f8d2edfcb35fc090cb0bcf177026066b7a3`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `segment-index-get-live:getHitSync` | `7216495.081 ops/s` | `2924480.501 ops/s` | `-59.48%` | `worse` |
| `segment-index-get-live:getMissSync` | `7033229.690 ops/s` | `2931525.617 ops/s` | `-58.32%` | `worse` |
| `segment-index-get-multisegment-cold:getHitSync` | `494576.984 ops/s` | `257307.989 ops/s` | `-47.97%` | `worse` |
| `segment-index-get-multisegment-cold:getMissSync` | `6743573.710 ops/s` | `2872531.264 ops/s` | `-57.40%` | `worse` |
| `segment-index-get-multisegment-hot:getHitSync` | `5783745.246 ops/s` | `2329684.420 ops/s` | `-59.72%` | `worse` |
| `segment-index-get-multisegment-hot:getMissSync` | `6447251.017 ops/s` | `2890150.910 ops/s` | `-55.17%` | `worse` |
| `segment-index-get-persisted:getHitSync` | `5468522.907 ops/s` | `2080472.958 ops/s` | `-61.96%` | `worse` |
| `segment-index-get-persisted:getMissSync` | `6237649.380 ops/s` | `3399118.543 ops/s` | `-45.51%` | `worse` |
| `segment-index-hot-route-put:putHotRoute` | `6305781.880 ops/s` | `2825902.086 ops/s` | `-55.19%` | `worse` |
| `segment-index-hot-route-put:putThenGetHotRoute` | `3320651.032 ops/s` | `1418092.087 ops/s` | `-57.29%` | `worse` |
| `segment-index-lifecycle:openAndCheckAndRepairConsistency` | `205.408 ms/op` | `133.618 ms/op` | `-34.95%` | `worse` |
| `segment-index-lifecycle:openAndCompact` | `217.612 ms/op` | `150.240 ms/op` | `-30.96%` | `worse` |
| `segment-index-lifecycle:openExisting` | `198.780 ms/op` | `131.212 ms/op` | `-33.99%` | `worse` |
| `segment-index-mixed-drain:partitionedIngestMixed` | `724960.633 ops/s` | `473219.925 ops/s` | `-34.72%` | `worse` |
| `segment-index-mixed-drain:partitionedIngestMixed:getWorkload` | `439306.599 ops/s` | `215428.622 ops/s` | `-50.96%` | `worse` |
| `segment-index-mixed-drain:partitionedIngestMixed:putWorkload` | `285654.034 ops/s` | `257791.303 ops/s` | `-9.75%` | `worse` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed` | `2011286.607 ops/s` | `929225.395 ops/s` | `-53.80%` | `worse` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:getWorkload` | `1960395.419 ops/s` | `892666.696 ops/s` | `-54.46%` | `worse` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:putWorkload` | `50891.189 ops/s` | `36558.699 ops/s` | `-28.16%` | `worse` |
| `segment-index-persisted-mutation-concurrent:deleteSync` | `8375.870 ops/s` | `9879.372 ops/s` | `+17.95%` | `better` |
| `segment-index-persisted-mutation-concurrent:putSync` | `7903.515 ops/s` | `9816.123 ops/s` | `+24.20%` | `better` |
| `segment-index-persisted-mutation:deleteSync` | `3616.737 ops/s` | `3647.428 ops/s` | `+0.85%` | `neutral` |
| `segment-index-persisted-mutation:putSync` | `3647.453 ops/s` | `3661.040 ops/s` | `+0.37%` | `neutral` |
| `segment-index-range-scan:boundedScan` | `20.010 us/op` | `25.143 us/op` | `+25.65%` | `better` |
| `segment-index-range-scan:fullStreamRangeFallback` | `1205.453 us/op` | `2000.873 us/op` | `+65.99%` | `better` |
| `segment-index-range-scan:sequentialRead` | `2338.313 us/op` | `4018.313 us/op` | `+71.85%` | `better` |
| `segment-merge-sequential:mergeSequential` | `184.189 us/op` | `338.009 us/op` | `+83.51%` | `better` |
