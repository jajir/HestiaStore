# Benchmark Comparison

- Profile: `segment-index-nightly`
- Baseline SHA: `6da25f8d2edfcb35fc090cb0bcf177026066b7a3`
- Candidate SHA: `6da25f8d2edfcb35fc090cb0bcf177026066b7a3`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `segment-index-get-live:getHitSync` | `2924480.501 ops/s` | `4535132.719 ops/s` | `+55.07%` | `better` |
| `segment-index-get-live:getMissSync` | `2931525.617 ops/s` | `4625192.240 ops/s` | `+57.77%` | `better` |
| `segment-index-get-multisegment-cold:getHitSync` | `257307.989 ops/s` | `279429.920 ops/s` | `+8.60%` | `better` |
| `segment-index-get-multisegment-cold:getMissSync` | `2872531.264 ops/s` | `4166999.011 ops/s` | `+45.06%` | `better` |
| `segment-index-get-multisegment-hot:getHitSync` | `2329684.420 ops/s` | `3451404.753 ops/s` | `+48.15%` | `better` |
| `segment-index-get-multisegment-hot:getMissSync` | `2890150.910 ops/s` | `4216876.398 ops/s` | `+45.91%` | `better` |
| `segment-index-get-persisted:getHitSync` | `2080472.958 ops/s` | `3307995.198 ops/s` | `+59.00%` | `better` |
| `segment-index-get-persisted:getMissSync` | `3399118.543 ops/s` | `4378019.253 ops/s` | `+28.80%` | `better` |
| `segment-index-hot-route-put:putHotRoute` | `2825902.086 ops/s` | `4164838.118 ops/s` | `+47.38%` | `better` |
| `segment-index-hot-route-put:putThenGetHotRoute` | `1418092.087 ops/s` | `2223947.824 ops/s` | `+56.83%` | `better` |
| `segment-index-lifecycle:openAndCheckAndRepairConsistency` | `133.618 ms/op` | `224.426 ms/op` | `+67.96%` | `better` |
| `segment-index-lifecycle:openAndCompact` | `150.240 ms/op` | `238.110 ms/op` | `+58.49%` | `better` |
| `segment-index-lifecycle:openExisting` | `131.212 ms/op` | `221.098 ms/op` | `+68.50%` | `better` |
| `segment-index-mixed-drain:partitionedIngestMixed` | `473219.925 ops/s` | `539037.315 ops/s` | `+13.91%` | `better` |
| `segment-index-mixed-drain:partitionedIngestMixed:getWorkload` | `215428.622 ops/s` | `250173.139 ops/s` | `+16.13%` | `better` |
| `segment-index-mixed-drain:partitionedIngestMixed:putWorkload` | `257791.303 ops/s` | `288864.175 ops/s` | `+12.05%` | `better` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed` | `929225.395 ops/s` | `1179786.161 ops/s` | `+26.96%` | `better` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:getWorkload` | `892666.696 ops/s` | `1145538.424 ops/s` | `+28.33%` | `better` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:putWorkload` | `36558.699 ops/s` | `34247.737 ops/s` | `-6.32%` | `warning` |
| `segment-index-persisted-mutation-concurrent:deleteSync` | `9879.372 ops/s` | `7957.805 ops/s` | `-19.45%` | `worse` |
| `segment-index-persisted-mutation-concurrent:putSync` | `9816.123 ops/s` | `7835.865 ops/s` | `-20.17%` | `worse` |
| `segment-index-persisted-mutation:deleteSync` | `3647.428 ops/s` | `3632.058 ops/s` | `-0.42%` | `neutral` |
| `segment-index-persisted-mutation:putSync` | `3661.040 ops/s` | `3539.875 ops/s` | `-3.31%` | `warning` |
| `segment-index-range-scan:boundedScan` | `25.143 us/op` | `27.444 us/op` | `+9.15%` | `better` |
| `segment-index-range-scan:fullStreamRangeFallback` | `2000.873 us/op` | `1973.402 us/op` | `-1.37%` | `neutral` |
| `segment-index-range-scan:sequentialRead` | `4018.313 us/op` | `3953.239 us/op` | `-1.62%` | `neutral` |
| `segment-merge-sequential:mergeSequential` | `338.009 us/op` | `333.120 us/op` | `-1.45%` | `neutral` |
