# Benchmark Comparison

- Profile: `segment-index-nightly`
- Baseline SHA: `6da25f8d2edfcb35fc090cb0bcf177026066b7a3`
- Candidate SHA: `6da25f8d2edfcb35fc090cb0bcf177026066b7a3`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `segment-index-get-live:getHitSync` | `4725422.319 ops/s` | `4349310.467 ops/s` | `-7.96%` | `worse` |
| `segment-index-get-live:getMissSync` | `4858385.219 ops/s` | `4273517.674 ops/s` | `-12.04%` | `worse` |
| `segment-index-get-multisegment-cold:getHitSync` | `259624.200 ops/s` | `272950.999 ops/s` | `+5.13%` | `better` |
| `segment-index-get-multisegment-cold:getMissSync` | `4256303.017 ops/s` | `4225052.818 ops/s` | `-0.73%` | `neutral` |
| `segment-index-get-multisegment-hot:getHitSync` | `3383484.884 ops/s` | `3368143.650 ops/s` | `-0.45%` | `neutral` |
| `segment-index-get-multisegment-hot:getMissSync` | `4409394.890 ops/s` | `3997461.515 ops/s` | `-9.34%` | `worse` |
| `segment-index-get-persisted:getHitSync` | `3370843.669 ops/s` | `3385325.406 ops/s` | `+0.43%` | `neutral` |
| `segment-index-get-persisted:getMissSync` | `4557294.334 ops/s` | `4522793.237 ops/s` | `-0.76%` | `neutral` |
| `segment-index-hot-route-put:putHotRoute` | `4128266.018 ops/s` | `4061343.140 ops/s` | `-1.62%` | `neutral` |
| `segment-index-hot-route-put:putThenGetHotRoute` | `2169057.023 ops/s` | `2077289.555 ops/s` | `-4.23%` | `warning` |
| `segment-index-lifecycle:openAndCheckAndRepairConsistency` | `274.934 ms/op` | `275.022 ms/op` | `+0.03%` | `neutral` |
| `segment-index-lifecycle:openAndCompact` | `293.708 ms/op` | `294.256 ms/op` | `+0.19%` | `neutral` |
| `segment-index-lifecycle:openExisting` | `273.760 ms/op` | `275.737 ms/op` | `+0.72%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed` | `578170.971 ops/s` | `550362.704 ops/s` | `-4.81%` | `warning` |
| `segment-index-mixed-drain:partitionedIngestMixed:getWorkload` | `285178.088 ops/s` | `258202.217 ops/s` | `-9.46%` | `worse` |
| `segment-index-mixed-drain:partitionedIngestMixed:putWorkload` | `292992.883 ops/s` | `292160.487 ops/s` | `-0.28%` | `neutral` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed` | `1201524.273 ops/s` | `1261013.298 ops/s` | `+4.95%` | `better` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:getWorkload` | `1165422.164 ops/s` | `1227051.028 ops/s` | `+5.29%` | `better` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:putWorkload` | `36102.110 ops/s` | `33962.271 ops/s` | `-5.93%` | `warning` |
| `segment-index-persisted-mutation-concurrent:deleteSync` | `8171.709 ops/s` | `7008.463 ops/s` | `-14.24%` | `worse` |
| `segment-index-persisted-mutation-concurrent:putSync` | `8243.716 ops/s` | `7124.075 ops/s` | `-13.58%` | `worse` |
| `segment-index-persisted-mutation:deleteSync` | `3622.643 ops/s` | `3073.413 ops/s` | `-15.16%` | `worse` |
| `segment-index-persisted-mutation:putSync` | `3530.549 ops/s` | `3150.008 ops/s` | `-10.78%` | `worse` |
| `segment-index-range-scan:boundedScan` | `27.527 us/op` | `30.839 us/op` | `+12.03%` | `better` |
| `segment-index-range-scan:fullStreamRangeFallback` | `2123.199 us/op` | `2156.720 us/op` | `+1.58%` | `neutral` |
| `segment-index-range-scan:sequentialRead` | `4167.632 us/op` | `4157.628 us/op` | `-0.24%` | `neutral` |
| `segment-merge-sequential:mergeSequential` | `358.467 us/op` | `358.818 us/op` | `+0.10%` | `neutral` |
