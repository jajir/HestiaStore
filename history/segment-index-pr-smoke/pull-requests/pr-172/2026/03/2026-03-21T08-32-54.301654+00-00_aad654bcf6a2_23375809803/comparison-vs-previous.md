# Benchmark Comparison

- Profile: `segment-index-pr-smoke`
- Baseline SHA: `e6adc34214c9ff0a3d8eae146bfacb8b07282732`
- Candidate SHA: `aad654bcf6a22049ef7394d14c674be72da8354c`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `segment-index-get-multisegment-hot:getHitAsyncJoin` | `88.460 ops/s` | `83.376 ops/s` | `-5.75%` | `warning` |
| `segment-index-get-multisegment-hot:getHitSync` | `87.764 ops/s` | `90.949 ops/s` | `+3.63%` | `better` |
| `segment-index-get-multisegment-hot:getMissAsyncJoin` | `162456.851 ops/s` | `172077.532 ops/s` | `+5.92%` | `better` |
| `segment-index-get-multisegment-hot:getMissSync` | `3802461.059 ops/s` | `3723557.463 ops/s` | `-2.08%` | `neutral` |
| `segment-index-get-overlay:getHitAsyncJoin` | `158685.938 ops/s` | `169424.368 ops/s` | `+6.77%` | `better` |
| `segment-index-get-overlay:getHitSync` | `4264371.226 ops/s` | `4327281.811 ops/s` | `+1.48%` | `neutral` |
| `segment-index-get-overlay:getMissAsyncJoin` | `164794.115 ops/s` | `171582.261 ops/s` | `+4.12%` | `better` |
| `segment-index-get-overlay:getMissSync` | `3908235.306 ops/s` | `3784171.632 ops/s` | `-3.17%` | `warning` |
| `segment-index-get-persisted:getHitAsyncJoin` | `54248.211 ops/s` | `59016.106 ops/s` | `+8.79%` | `better` |
| `segment-index-get-persisted:getHitSync` | `102377.920 ops/s` | `110072.270 ops/s` | `+7.52%` | `better` |
| `segment-index-get-persisted:getMissAsyncJoin` | `159267.800 ops/s` | `172520.975 ops/s` | `+8.32%` | `better` |
| `segment-index-get-persisted:getMissSync` | `3885076.470 ops/s` | `3909165.720 ops/s` | `+0.62%` | `neutral` |
| `segment-index-hot-partition-put:putHotPartition` | `3155634.197 ops/s` | `3058114.505 ops/s` | `-3.09%` | `warning` |
| `segment-index-hot-partition-put:putThenGetHotPartition` | `1669155.100 ops/s` | `1693267.231 ops/s` | `+1.44%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed` | `436911.203 ops/s` | `453428.586 ops/s` | `+3.78%` | `better` |
| `segment-index-mixed-drain:partitionedIngestMixed:getWorkload` | `431746.206 ops/s` | `448259.327 ops/s` | `+3.82%` | `better` |
| `segment-index-mixed-drain:partitionedIngestMixed:putWorkload` | `5164.997 ops/s` | `5169.259 ops/s` | `+0.08%` | `neutral` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed` | `204935.041 ops/s` | `208410.001 ops/s` | `+1.70%` | `neutral` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:getWorkload` | `202333.829 ops/s` | `205780.544 ops/s` | `+1.70%` | `neutral` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:putWorkload` | `2601.212 ops/s` | `2629.457 ops/s` | `+1.09%` | `neutral` |
| `segment-index-persisted-mutation:deleteAsyncJoin` | `2128.390 ops/s` | `2348.439 ops/s` | `+10.34%` | `better` |
| `segment-index-persisted-mutation:deleteSync` | `2324.270 ops/s` | `2661.222 ops/s` | `+14.50%` | `better` |
| `segment-index-persisted-mutation:putAsyncJoin` | `2044.173 ops/s` | `2312.843 ops/s` | `+13.14%` | `better` |
| `segment-index-persisted-mutation:putSync` | `2235.482 ops/s` | `2540.306 ops/s` | `+13.64%` | `better` |
