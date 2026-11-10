# Benchmark Comparison

- Profile: `segment-index-nightly`
- Baseline SHA: `6da25f8d2edfcb35fc090cb0bcf177026066b7a3`
- Candidate SHA: `6da25f8d2edfcb35fc090cb0bcf177026066b7a3`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `segment-index-get-live:getHitSync` | `4800335.211 ops/s` | `2853679.229 ops/s` | `-40.55%` | `worse` |
| `segment-index-get-live:getMissSync` | `4554064.138 ops/s` | `2991133.759 ops/s` | `-34.32%` | `worse` |
| `segment-index-get-multisegment-cold:getHitSync` | `270937.824 ops/s` | `247883.065 ops/s` | `-8.51%` | `worse` |
| `segment-index-get-multisegment-cold:getMissSync` | `4205274.778 ops/s` | `2775828.922 ops/s` | `-33.99%` | `worse` |
| `segment-index-get-multisegment-hot:getHitSync` | `3309106.774 ops/s` | `2132544.818 ops/s` | `-35.56%` | `worse` |
| `segment-index-get-multisegment-hot:getMissSync` | `3378470.212 ops/s` | `3190965.598 ops/s` | `-5.55%` | `warning` |
| `segment-index-get-persisted:getHitSync` | `3072767.995 ops/s` | `2069116.248 ops/s` | `-32.66%` | `worse` |
| `segment-index-get-persisted:getMissSync` | `4206836.644 ops/s` | `2862668.178 ops/s` | `-31.95%` | `worse` |
| `segment-index-hot-route-put:putHotRoute` | `4052317.510 ops/s` | `2819452.298 ops/s` | `-30.42%` | `worse` |
| `segment-index-hot-route-put:putThenGetHotRoute` | `2074773.792 ops/s` | `1555671.061 ops/s` | `-25.02%` | `worse` |
| `segment-index-lifecycle:openAndCheckAndRepairConsistency` | `227.994 ms/op` | `119.518 ms/op` | `-47.58%` | `worse` |
| `segment-index-lifecycle:openAndCompact` | `242.377 ms/op` | `135.755 ms/op` | `-43.99%` | `worse` |
| `segment-index-lifecycle:openExisting` | `225.556 ms/op` | `115.181 ms/op` | `-48.93%` | `worse` |
| `segment-index-mixed-drain:partitionedIngestMixed` | `497415.430 ops/s` | `456522.099 ops/s` | `-8.22%` | `worse` |
| `segment-index-mixed-drain:partitionedIngestMixed:getWorkload` | `248665.790 ops/s` | `198821.675 ops/s` | `-20.04%` | `worse` |
| `segment-index-mixed-drain:partitionedIngestMixed:putWorkload` | `248749.640 ops/s` | `257700.424 ops/s` | `+3.60%` | `better` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed` | `1110352.803 ops/s` | `845986.009 ops/s` | `-23.81%` | `worse` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:getWorkload` | `1079095.857 ops/s` | `808798.112 ops/s` | `-25.05%` | `worse` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:putWorkload` | `31256.945 ops/s` | `37187.897 ops/s` | `+18.97%` | `better` |
| `segment-index-persisted-mutation-concurrent:deleteSync` | `6204.939 ops/s` | `8512.516 ops/s` | `+37.19%` | `better` |
| `segment-index-persisted-mutation-concurrent:putSync` | `6053.535 ops/s` | `8663.952 ops/s` | `+43.12%` | `better` |
| `segment-index-persisted-mutation:deleteSync` | `2794.692 ops/s` | `3784.070 ops/s` | `+35.40%` | `better` |
| `segment-index-persisted-mutation:putSync` | `2672.083 ops/s` | `3593.520 ops/s` | `+34.48%` | `better` |
| `segment-index-range-scan:boundedScan` | `29.558 us/op` | `27.280 us/op` | `-7.71%` | `worse` |
| `segment-index-range-scan:fullStreamRangeFallback` | `2031.744 us/op` | `2025.669 us/op` | `-0.30%` | `neutral` |
| `segment-index-range-scan:sequentialRead` | `3997.761 us/op` | `3970.682 us/op` | `-0.68%` | `neutral` |
| `segment-merge-sequential:mergeSequential` | `334.970 us/op` | `336.152 us/op` | `+0.35%` | `neutral` |
