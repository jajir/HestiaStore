# Benchmark Comparison

- Profile: `segment-index-nightly`
- Baseline SHA: `6da25f8d2edfcb35fc090cb0bcf177026066b7a3`
- Candidate SHA: `6da25f8d2edfcb35fc090cb0bcf177026066b7a3`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `segment-index-get-live:getHitSync` | `4861775.811 ops/s` | `4725422.319 ops/s` | `-2.80%` | `neutral` |
| `segment-index-get-live:getMissSync` | `5053263.471 ops/s` | `4858385.219 ops/s` | `-3.86%` | `warning` |
| `segment-index-get-multisegment-cold:getHitSync` | `278522.897 ops/s` | `259624.200 ops/s` | `-6.79%` | `warning` |
| `segment-index-get-multisegment-cold:getMissSync` | `4546816.362 ops/s` | `4256303.017 ops/s` | `-6.39%` | `warning` |
| `segment-index-get-multisegment-hot:getHitSync` | `3628287.717 ops/s` | `3383484.884 ops/s` | `-6.75%` | `warning` |
| `segment-index-get-multisegment-hot:getMissSync` | `4678917.495 ops/s` | `4409394.890 ops/s` | `-5.76%` | `warning` |
| `segment-index-get-persisted:getHitSync` | `3510311.456 ops/s` | `3370843.669 ops/s` | `-3.97%` | `warning` |
| `segment-index-get-persisted:getMissSync` | `5007874.209 ops/s` | `4557294.334 ops/s` | `-9.00%` | `worse` |
| `segment-index-hot-route-put:putHotRoute` | `4283421.186 ops/s` | `4128266.018 ops/s` | `-3.62%` | `warning` |
| `segment-index-hot-route-put:putThenGetHotRoute` | `2301328.384 ops/s` | `2169057.023 ops/s` | `-5.75%` | `warning` |
| `segment-index-lifecycle:openAndCheckAndRepairConsistency` | `239.311 ms/op` | `274.934 ms/op` | `+14.89%` | `better` |
| `segment-index-lifecycle:openAndCompact` | `256.752 ms/op` | `293.708 ms/op` | `+14.39%` | `better` |
| `segment-index-lifecycle:openExisting` | `236.631 ms/op` | `273.760 ms/op` | `+15.69%` | `better` |
| `segment-index-mixed-drain:partitionedIngestMixed` | `558194.207 ops/s` | `578170.971 ops/s` | `+3.58%` | `better` |
| `segment-index-mixed-drain:partitionedIngestMixed:getWorkload` | `259427.128 ops/s` | `285178.088 ops/s` | `+9.93%` | `better` |
| `segment-index-mixed-drain:partitionedIngestMixed:putWorkload` | `298767.079 ops/s` | `292992.883 ops/s` | `-1.93%` | `neutral` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed` | `1275527.997 ops/s` | `1201524.273 ops/s` | `-5.80%` | `warning` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:getWorkload` | `1241610.469 ops/s` | `1165422.164 ops/s` | `-6.14%` | `warning` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:putWorkload` | `33917.529 ops/s` | `36102.110 ops/s` | `+6.44%` | `better` |
| `segment-index-persisted-mutation-concurrent:deleteSync` | `4317.202 ops/s` | `8171.709 ops/s` | `+89.28%` | `better` |
| `segment-index-persisted-mutation-concurrent:putSync` | `4600.747 ops/s` | `8243.716 ops/s` | `+79.18%` | `better` |
| `segment-index-persisted-mutation:deleteSync` | `1788.939 ops/s` | `3622.643 ops/s` | `+102.50%` | `better` |
| `segment-index-persisted-mutation:putSync` | `1742.404 ops/s` | `3530.549 ops/s` | `+102.63%` | `better` |
| `segment-index-range-scan:boundedScan` | `33.153 us/op` | `27.527 us/op` | `-16.97%` | `worse` |
| `segment-index-range-scan:fullStreamRangeFallback` | `2004.221 us/op` | `2123.199 us/op` | `+5.94%` | `better` |
| `segment-index-range-scan:sequentialRead` | `3778.021 us/op` | `4167.632 us/op` | `+10.31%` | `better` |
| `segment-merge-sequential:mergeSequential` | `331.308 us/op` | `358.467 us/op` | `+8.20%` | `better` |
