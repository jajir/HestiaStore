# Benchmark Comparison

- Profile: `segment-index-pr-smoke`
- Baseline SHA: `baa631b9aeb3dde8a7971d7b3ec854f5fa53ff07`
- Candidate SHA: `94b88ea3b61456a03f85f4753ee065935eb784cb`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `segment-index-get-live:getHitSync` | `5169230.049 ops/s` | `4893145.875 ops/s` | `-5.34%` | `warning` |
| `segment-index-get-live:getMissSync` | `4835118.360 ops/s` | `4662887.068 ops/s` | `-3.56%` | `warning` |
| `segment-index-get-multisegment-hot:getHitSync` | `3451656.172 ops/s` | `3494369.887 ops/s` | `+1.24%` | `neutral` |
| `segment-index-get-multisegment-hot:getMissSync` | `4778859.820 ops/s` | `4166570.142 ops/s` | `-12.81%` | `worse` |
| `segment-index-get-persisted:getHitSync` | `3358557.469 ops/s` | `3362663.731 ops/s` | `+0.12%` | `neutral` |
| `segment-index-get-persisted:getMissSync` | `4780629.071 ops/s` | `4476060.226 ops/s` | `-6.37%` | `warning` |
| `segment-index-hot-route-put:putHotRoute` | `4239451.001 ops/s` | `4016332.734 ops/s` | `-5.26%` | `warning` |
| `segment-index-hot-route-put:putThenGetHotRoute` | `2211979.147 ops/s` | `2173446.168 ops/s` | `-1.74%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed` | `552384.954 ops/s` | `574291.916 ops/s` | `+3.97%` | `better` |
| `segment-index-mixed-drain:partitionedIngestMixed:getWorkload` | `384317.055 ops/s` | `396900.504 ops/s` | `+3.27%` | `better` |
| `segment-index-mixed-drain:partitionedIngestMixed:putWorkload` | `168067.899 ops/s` | `177391.411 ops/s` | `+5.55%` | `better` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed` | `742435.014 ops/s` | `690293.725 ops/s` | `-7.02%` | `worse` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:getWorkload` | `728907.378 ops/s` | `677771.390 ops/s` | `-7.02%` | `worse` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:putWorkload` | `13527.636 ops/s` | `12522.335 ops/s` | `-7.43%` | `worse` |
| `segment-index-persisted-mutation-concurrent:deleteSync` | `6681.408 ops/s` | `7488.883 ops/s` | `+12.09%` | `better` |
| `segment-index-persisted-mutation-concurrent:putSync` | `6538.217 ops/s` | `7272.689 ops/s` | `+11.23%` | `better` |
| `segment-index-persisted-mutation:deleteSync` | `2763.887 ops/s` | `3651.910 ops/s` | `+32.13%` | `better` |
| `segment-index-persisted-mutation:putSync` | `2640.453 ops/s` | `3542.111 ops/s` | `+34.15%` | `better` |
| `segment-index-range-scan:boundedScan` | `28.403 us/op` | `40.513 us/op` | `+42.64%` | `better` |
| `segment-index-range-scan:fullStreamRangeFallback` | `2128.094 us/op` | `2260.530 us/op` | `+6.22%` | `better` |
| `segment-index-range-scan:sequentialRead` | `4074.849 us/op` | `4426.871 us/op` | `+8.64%` | `better` |
| `segment-merge-sequential:mergeSequential` | `325.873 us/op` | `351.195 us/op` | `+7.77%` | `better` |
