# Benchmark Comparison

- Profile: `segment-index-pr-smoke`
- Baseline SHA: `6652c9e98795c96934f1ef0953f240800c3ae3e9`
- Candidate SHA: `3ea88b07c74db15f5b72eb8054a63f7d1373ff9a`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `segment-index-get-live:getHitAsyncJoin` | `152970.554 ops/s` | `-` | `-` | `removed` |
| `segment-index-get-live:getHitSync` | `3833838.729 ops/s` | `3648477.729 ops/s` | `-4.83%` | `warning` |
| `segment-index-get-live:getMissAsyncJoin` | `162875.950 ops/s` | `-` | `-` | `removed` |
| `segment-index-get-live:getMissSync` | `4065533.531 ops/s` | `3947083.553 ops/s` | `-2.91%` | `neutral` |
| `segment-index-get-multisegment-hot:getHitAsyncJoin` | `74.062 ops/s` | `-` | `-` | `removed` |
| `segment-index-get-multisegment-hot:getHitSync` | `91.846 ops/s` | `92.402 ops/s` | `+0.61%` | `neutral` |
| `segment-index-get-multisegment-hot:getMissAsyncJoin` | `169365.215 ops/s` | `-` | `-` | `removed` |
| `segment-index-get-multisegment-hot:getMissSync` | `3934249.825 ops/s` | `4046586.986 ops/s` | `+2.86%` | `neutral` |
| `segment-index-get-persisted:getHitAsyncJoin` | `52284.056 ops/s` | `-` | `-` | `removed` |
| `segment-index-get-persisted:getHitSync` | `107998.190 ops/s` | `92805.686 ops/s` | `-14.07%` | `worse` |
| `segment-index-get-persisted:getMissAsyncJoin` | `163013.053 ops/s` | `-` | `-` | `removed` |
| `segment-index-get-persisted:getMissSync` | `3972648.106 ops/s` | `4129974.384 ops/s` | `+3.96%` | `better` |
| `segment-index-hot-route-put:putHotRoute` | `2288932.890 ops/s` | `2529775.781 ops/s` | `+10.52%` | `better` |
| `segment-index-hot-route-put:putThenGetHotRoute` | `1420638.928 ops/s` | `1472840.514 ops/s` | `+3.67%` | `better` |
| `segment-index-mixed-drain:partitionedIngestMixed` | `341033.238 ops/s` | `392856.528 ops/s` | `+15.20%` | `better` |
| `segment-index-mixed-drain:partitionedIngestMixed:getWorkload` | `179077.397 ops/s` | `239441.898 ops/s` | `+33.71%` | `better` |
| `segment-index-mixed-drain:partitionedIngestMixed:putWorkload` | `161955.840 ops/s` | `153414.630 ops/s` | `-5.27%` | `warning` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed` | `31809.442 ops/s` | `43913.405 ops/s` | `+38.05%` | `better` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:getWorkload` | `23722.589 ops/s` | `38500.502 ops/s` | `+62.29%` | `better` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:putWorkload` | `8086.853 ops/s` | `5412.903 ops/s` | `-33.07%` | `worse` |
| `segment-index-persisted-mutation:deleteAsyncJoin` | `2317.075 ops/s` | `-` | `-` | `removed` |
| `segment-index-persisted-mutation:deleteSync` | `2707.995 ops/s` | `2690.141 ops/s` | `-0.66%` | `neutral` |
| `segment-index-persisted-mutation:putAsyncJoin` | `2288.449 ops/s` | `-` | `-` | `removed` |
| `segment-index-persisted-mutation:putSync` | `2647.470 ops/s` | `2526.230 ops/s` | `-4.58%` | `warning` |
