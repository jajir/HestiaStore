# Benchmark Comparison

- Profile: `segment-index-nightly`
- Baseline SHA: `6da25f8d2edfcb35fc090cb0bcf177026066b7a3`
- Candidate SHA: `6da25f8d2edfcb35fc090cb0bcf177026066b7a3`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `segment-index-get-live:getHitSync` | `6392803.869 ops/s` | `5012443.535 ops/s` | `-21.59%` | `worse` |
| `segment-index-get-live:getMissSync` | `6822561.616 ops/s` | `4678745.618 ops/s` | `-31.42%` | `worse` |
| `segment-index-get-multisegment-cold:getHitSync` | `512362.036 ops/s` | `281650.649 ops/s` | `-45.03%` | `worse` |
| `segment-index-get-multisegment-cold:getMissSync` | `7123153.696 ops/s` | `4900198.072 ops/s` | `-31.21%` | `worse` |
| `segment-index-get-multisegment-hot:getHitSync` | `5688281.599 ops/s` | `3679481.405 ops/s` | `-35.31%` | `worse` |
| `segment-index-get-multisegment-hot:getMissSync` | `6349925.919 ops/s` | `4103574.031 ops/s` | `-35.38%` | `worse` |
| `segment-index-get-persisted:getHitSync` | `5183059.094 ops/s` | `3668278.597 ops/s` | `-29.23%` | `worse` |
| `segment-index-get-persisted:getMissSync` | `6801838.644 ops/s` | `4895628.553 ops/s` | `-28.02%` | `worse` |
| `segment-index-hot-route-put:putHotRoute` | `6702548.147 ops/s` | `4728287.748 ops/s` | `-29.46%` | `worse` |
| `segment-index-hot-route-put:putThenGetHotRoute` | `3484457.201 ops/s` | `2277668.774 ops/s` | `-34.63%` | `worse` |
| `segment-index-lifecycle:openAndCheckAndRepairConsistency` | `199.965 ms/op` | `245.750 ms/op` | `+22.90%` | `better` |
| `segment-index-lifecycle:openAndCompact` | `215.613 ms/op` | `260.550 ms/op` | `+20.84%` | `better` |
| `segment-index-lifecycle:openExisting` | `204.719 ms/op` | `241.618 ms/op` | `+18.02%` | `better` |
| `segment-index-mixed-drain:partitionedIngestMixed` | `720784.300 ops/s` | `538105.599 ops/s` | `-25.34%` | `worse` |
| `segment-index-mixed-drain:partitionedIngestMixed:getWorkload` | `432936.779 ops/s` | `248357.031 ops/s` | `-42.63%` | `worse` |
| `segment-index-mixed-drain:partitionedIngestMixed:putWorkload` | `287847.521 ops/s` | `289748.568 ops/s` | `+0.66%` | `neutral` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed` | `1869981.865 ops/s` | `1254161.503 ops/s` | `-32.93%` | `worse` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:getWorkload` | `1817994.794 ops/s` | `1220426.181 ops/s` | `-32.87%` | `worse` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:putWorkload` | `51987.072 ops/s` | `33735.322 ops/s` | `-35.11%` | `worse` |
| `segment-index-persisted-mutation-concurrent:deleteSync` | `7819.730 ops/s` | `8795.036 ops/s` | `+12.47%` | `better` |
| `segment-index-persisted-mutation-concurrent:putSync` | `8200.343 ops/s` | `8814.218 ops/s` | `+7.49%` | `better` |
| `segment-index-persisted-mutation:deleteSync` | `3763.363 ops/s` | `3259.133 ops/s` | `-13.40%` | `worse` |
| `segment-index-persisted-mutation:putSync` | `3710.498 ops/s` | `3098.436 ops/s` | `-16.50%` | `worse` |
| `segment-index-range-scan:boundedScan` | `16.896 us/op` | `34.306 us/op` | `+103.05%` | `better` |
| `segment-index-range-scan:fullStreamRangeFallback` | `1180.993 us/op` | `1968.568 us/op` | `+66.69%` | `better` |
| `segment-index-range-scan:sequentialRead` | `2286.547 us/op` | `3933.202 us/op` | `+72.01%` | `better` |
| `segment-merge-sequential:mergeSequential` | `179.766 us/op` | `334.567 us/op` | `+86.11%` | `better` |
