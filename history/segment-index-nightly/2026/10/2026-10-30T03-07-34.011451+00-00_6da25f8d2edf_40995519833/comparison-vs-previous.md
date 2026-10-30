# Benchmark Comparison

- Profile: `segment-index-nightly`
- Baseline SHA: `6da25f8d2edfcb35fc090cb0bcf177026066b7a3`
- Candidate SHA: `6da25f8d2edfcb35fc090cb0bcf177026066b7a3`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `segment-index-get-live:getHitSync` | `4535132.719 ops/s` | `6693322.619 ops/s` | `+47.59%` | `better` |
| `segment-index-get-live:getMissSync` | `4625192.240 ops/s` | `6775017.762 ops/s` | `+46.48%` | `better` |
| `segment-index-get-multisegment-cold:getHitSync` | `279429.920 ops/s` | `491666.569 ops/s` | `+75.95%` | `better` |
| `segment-index-get-multisegment-cold:getMissSync` | `4166999.011 ops/s` | `6417103.693 ops/s` | `+54.00%` | `better` |
| `segment-index-get-multisegment-hot:getHitSync` | `3451404.753 ops/s` | `5708409.434 ops/s` | `+65.39%` | `better` |
| `segment-index-get-multisegment-hot:getMissSync` | `4216876.398 ops/s` | `6693228.791 ops/s` | `+58.72%` | `better` |
| `segment-index-get-persisted:getHitSync` | `3307995.198 ops/s` | `5834099.901 ops/s` | `+76.36%` | `better` |
| `segment-index-get-persisted:getMissSync` | `4378019.253 ops/s` | `6720773.328 ops/s` | `+53.51%` | `better` |
| `segment-index-hot-route-put:putHotRoute` | `4164838.118 ops/s` | `6189535.567 ops/s` | `+48.61%` | `better` |
| `segment-index-hot-route-put:putThenGetHotRoute` | `2223947.824 ops/s` | `3026954.197 ops/s` | `+36.11%` | `better` |
| `segment-index-lifecycle:openAndCheckAndRepairConsistency` | `224.426 ms/op` | `191.114 ms/op` | `-14.84%` | `worse` |
| `segment-index-lifecycle:openAndCompact` | `238.110 ms/op` | `198.597 ms/op` | `-16.59%` | `worse` |
| `segment-index-lifecycle:openExisting` | `221.098 ms/op` | `185.589 ms/op` | `-16.06%` | `worse` |
| `segment-index-mixed-drain:partitionedIngestMixed` | `539037.315 ops/s` | `727517.878 ops/s` | `+34.97%` | `better` |
| `segment-index-mixed-drain:partitionedIngestMixed:getWorkload` | `250173.139 ops/s` | `439641.200 ops/s` | `+75.73%` | `better` |
| `segment-index-mixed-drain:partitionedIngestMixed:putWorkload` | `288864.175 ops/s` | `287876.678 ops/s` | `-0.34%` | `neutral` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed` | `1179786.161 ops/s` | `2008809.990 ops/s` | `+70.27%` | `better` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:getWorkload` | `1145538.424 ops/s` | `1957980.603 ops/s` | `+70.92%` | `better` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:putWorkload` | `34247.737 ops/s` | `50829.387 ops/s` | `+48.42%` | `better` |
| `segment-index-persisted-mutation-concurrent:deleteSync` | `7957.805 ops/s` | `7954.222 ops/s` | `-0.05%` | `neutral` |
| `segment-index-persisted-mutation-concurrent:putSync` | `7835.865 ops/s` | `7453.503 ops/s` | `-4.88%` | `warning` |
| `segment-index-persisted-mutation:deleteSync` | `3632.058 ops/s` | `3835.687 ops/s` | `+5.61%` | `better` |
| `segment-index-persisted-mutation:putSync` | `3539.875 ops/s` | `3586.610 ops/s` | `+1.32%` | `neutral` |
| `segment-index-range-scan:boundedScan` | `27.444 us/op` | `18.328 us/op` | `-33.22%` | `worse` |
| `segment-index-range-scan:fullStreamRangeFallback` | `1973.402 us/op` | `1182.288 us/op` | `-40.09%` | `worse` |
| `segment-index-range-scan:sequentialRead` | `3953.239 us/op` | `2262.133 us/op` | `-42.78%` | `worse` |
| `segment-merge-sequential:mergeSequential` | `333.120 us/op` | `181.518 us/op` | `-45.51%` | `worse` |
