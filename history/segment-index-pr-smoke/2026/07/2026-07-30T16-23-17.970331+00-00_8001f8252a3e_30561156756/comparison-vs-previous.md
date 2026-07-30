# Benchmark Comparison

- Profile: `segment-index-pr-smoke`
- Baseline SHA: `baa631b9aeb3dde8a7971d7b3ec854f5fa53ff07`
- Candidate SHA: `8001f8252a3e24c28f05deafffa0fc6b4ef3247a`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `segment-index-get-live:getHitSync` | `5169230.049 ops/s` | `6240663.082 ops/s` | `+20.73%` | `better` |
| `segment-index-get-live:getMissSync` | `4835118.360 ops/s` | `5850987.323 ops/s` | `+21.01%` | `better` |
| `segment-index-get-multisegment-hot:getHitSync` | `3451656.172 ops/s` | `4512735.115 ops/s` | `+30.74%` | `better` |
| `segment-index-get-multisegment-hot:getMissSync` | `4778859.820 ops/s` | `5706234.455 ops/s` | `+19.41%` | `better` |
| `segment-index-get-persisted:getHitSync` | `3358557.469 ops/s` | `4288156.460 ops/s` | `+27.68%` | `better` |
| `segment-index-get-persisted:getMissSync` | `4780629.071 ops/s` | `6055495.024 ops/s` | `+26.67%` | `better` |
| `segment-index-hot-route-put:putHotRoute` | `4239451.001 ops/s` | `5270032.109 ops/s` | `+24.31%` | `better` |
| `segment-index-hot-route-put:putThenGetHotRoute` | `2211979.147 ops/s` | `2745576.244 ops/s` | `+24.12%` | `better` |
| `segment-index-mixed-drain:partitionedIngestMixed` | `552384.954 ops/s` | `615150.736 ops/s` | `+11.36%` | `better` |
| `segment-index-mixed-drain:partitionedIngestMixed:getWorkload` | `384317.055 ops/s` | `384846.685 ops/s` | `+0.14%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed:putWorkload` | `168067.899 ops/s` | `230304.050 ops/s` | `+37.03%` | `better` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed` | `742435.014 ops/s` | `1291019.329 ops/s` | `+73.89%` | `better` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:getWorkload` | `728907.378 ops/s` | `1275825.433 ops/s` | `+75.03%` | `better` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:putWorkload` | `13527.636 ops/s` | `15193.896 ops/s` | `+12.32%` | `better` |
| `segment-index-persisted-mutation-concurrent:deleteSync` | `6681.408 ops/s` | `5363.318 ops/s` | `-19.73%` | `worse` |
| `segment-index-persisted-mutation-concurrent:putSync` | `6538.217 ops/s` | `5477.726 ops/s` | `-16.22%` | `worse` |
| `segment-index-persisted-mutation:deleteSync` | `2763.887 ops/s` | `2691.927 ops/s` | `-2.60%` | `neutral` |
| `segment-index-persisted-mutation:putSync` | `2640.453 ops/s` | `2492.139 ops/s` | `-5.62%` | `warning` |
| `segment-index-range-scan:boundedScan` | `28.403 us/op` | `23.949 us/op` | `-15.68%` | `worse` |
| `segment-index-range-scan:fullStreamRangeFallback` | `2128.094 us/op` | `1651.722 us/op` | `-22.38%` | `worse` |
| `segment-index-range-scan:sequentialRead` | `4074.849 us/op` | `3167.353 us/op` | `-22.27%` | `worse` |
| `segment-merge-sequential:mergeSequential` | `325.873 us/op` | `278.044 us/op` | `-14.68%` | `worse` |
