# Benchmark Comparison

- Candidate labels rerun 3x total and median-merged because the initial regression exceeded threshold with high relative score error: `segment-index-get-multisegment-hot`


- Profile: `segment-index-pr-smoke`
- Baseline SHA: `e6adc34214c9ff0a3d8eae146bfacb8b07282732`
- Candidate SHA: `3b6cd8c844bcf5fbd4cae8e281f199d5c91fe524`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `segment-index-get-multisegment-hot:getHitAsyncJoin` | `88.460 ops/s` | `82.823 ops/s` | `-6.37%` | `warning` |
| `segment-index-get-multisegment-hot:getHitSync` | `87.764 ops/s` | `83.926 ops/s` | `-4.37%` | `warning` |
| `segment-index-get-multisegment-hot:getMissAsyncJoin` | `162456.851 ops/s` | `168774.238 ops/s` | `+3.89%` | `better` |
| `segment-index-get-multisegment-hot:getMissSync` | `3802461.059 ops/s` | `3860250.751 ops/s` | `+1.52%` | `neutral` |
| `segment-index-get-overlay:getHitAsyncJoin` | `158685.938 ops/s` | `173630.590 ops/s` | `+9.42%` | `better` |
| `segment-index-get-overlay:getHitSync` | `4264371.226 ops/s` | `4387462.784 ops/s` | `+2.89%` | `neutral` |
| `segment-index-get-overlay:getMissAsyncJoin` | `164794.115 ops/s` | `174403.328 ops/s` | `+5.83%` | `better` |
| `segment-index-get-overlay:getMissSync` | `3908235.306 ops/s` | `3856114.306 ops/s` | `-1.33%` | `neutral` |
| `segment-index-get-persisted:getHitAsyncJoin` | `54248.211 ops/s` | `57402.091 ops/s` | `+5.81%` | `better` |
| `segment-index-get-persisted:getHitSync` | `102377.920 ops/s` | `111229.044 ops/s` | `+8.65%` | `better` |
| `segment-index-get-persisted:getMissAsyncJoin` | `159267.800 ops/s` | `177555.385 ops/s` | `+11.48%` | `better` |
| `segment-index-get-persisted:getMissSync` | `3885076.470 ops/s` | `4356629.518 ops/s` | `+12.14%` | `better` |
| `segment-index-hot-partition-put:putHotPartition` | `3155634.197 ops/s` | `3215883.649 ops/s` | `+1.91%` | `neutral` |
| `segment-index-hot-partition-put:putThenGetHotPartition` | `1669155.100 ops/s` | `1741538.462 ops/s` | `+4.34%` | `better` |
| `segment-index-mixed-drain:partitionedIngestMixed` | `436911.203 ops/s` | `507358.394 ops/s` | `+16.12%` | `better` |
| `segment-index-mixed-drain:partitionedIngestMixed:getWorkload` | `431746.206 ops/s` | `502107.544 ops/s` | `+16.30%` | `better` |
| `segment-index-mixed-drain:partitionedIngestMixed:putWorkload` | `5164.997 ops/s` | `5250.850 ops/s` | `+1.66%` | `neutral` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed` | `204935.041 ops/s` | `196481.602 ops/s` | `-4.12%` | `warning` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:getWorkload` | `202333.829 ops/s` | `193977.280 ops/s` | `-4.13%` | `warning` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:putWorkload` | `2601.212 ops/s` | `2504.322 ops/s` | `-3.72%` | `warning` |
| `segment-index-persisted-mutation:deleteAsyncJoin` | `2128.390 ops/s` | `2339.510 ops/s` | `+9.92%` | `better` |
| `segment-index-persisted-mutation:deleteSync` | `2324.270 ops/s` | `2588.790 ops/s` | `+11.38%` | `better` |
| `segment-index-persisted-mutation:putAsyncJoin` | `2044.173 ops/s` | `2415.415 ops/s` | `+18.16%` | `better` |
| `segment-index-persisted-mutation:putSync` | `2235.482 ops/s` | `2597.925 ops/s` | `+16.21%` | `better` |
