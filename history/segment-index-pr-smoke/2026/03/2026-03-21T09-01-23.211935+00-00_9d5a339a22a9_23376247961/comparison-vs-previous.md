# Benchmark Comparison

- Candidate labels rerun 3x total and median-merged because the initial regression exceeded threshold with high relative score error: `segment-index-get-persisted`


- Profile: `segment-index-pr-smoke`
- Baseline SHA: `e6adc34214c9ff0a3d8eae146bfacb8b07282732`
- Candidate SHA: `9d5a339a22a9b41105834c8ae9ce3405bf76481c`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `segment-index-get-multisegment-hot:getHitAsyncJoin` | `88.460 ops/s` | `87.887 ops/s` | `-0.65%` | `neutral` |
| `segment-index-get-multisegment-hot:getHitSync` | `87.764 ops/s` | `90.182 ops/s` | `+2.75%` | `neutral` |
| `segment-index-get-multisegment-hot:getMissAsyncJoin` | `162456.851 ops/s` | `165003.911 ops/s` | `+1.57%` | `neutral` |
| `segment-index-get-multisegment-hot:getMissSync` | `3802461.059 ops/s` | `3842175.204 ops/s` | `+1.04%` | `neutral` |
| `segment-index-get-overlay:getHitAsyncJoin` | `158685.938 ops/s` | `156383.765 ops/s` | `-1.45%` | `neutral` |
| `segment-index-get-overlay:getHitSync` | `4264371.226 ops/s` | `4242771.883 ops/s` | `-0.51%` | `neutral` |
| `segment-index-get-overlay:getMissAsyncJoin` | `164794.115 ops/s` | `158384.833 ops/s` | `-3.89%` | `warning` |
| `segment-index-get-overlay:getMissSync` | `3908235.306 ops/s` | `3927878.358 ops/s` | `+0.50%` | `neutral` |
| `segment-index-get-persisted:getHitAsyncJoin` | `54248.211 ops/s` | `56455.848 ops/s` | `+4.07%` | `better` |
| `segment-index-get-persisted:getHitSync` | `102377.920 ops/s` | `106054.215 ops/s` | `+3.59%` | `better` |
| `segment-index-get-persisted:getMissAsyncJoin` | `159267.800 ops/s` | `163018.136 ops/s` | `+2.35%` | `neutral` |
| `segment-index-get-persisted:getMissSync` | `3885076.470 ops/s` | `3859341.319 ops/s` | `-0.66%` | `neutral` |
| `segment-index-hot-partition-put:putHotPartition` | `3155634.197 ops/s` | `3028321.041 ops/s` | `-4.03%` | `warning` |
| `segment-index-hot-partition-put:putThenGetHotPartition` | `1669155.100 ops/s` | `1664173.925 ops/s` | `-0.30%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed` | `436911.203 ops/s` | `407462.667 ops/s` | `-6.74%` | `warning` |
| `segment-index-mixed-drain:partitionedIngestMixed:getWorkload` | `431746.206 ops/s` | `402274.749 ops/s` | `-6.83%` | `warning` |
| `segment-index-mixed-drain:partitionedIngestMixed:putWorkload` | `5164.997 ops/s` | `5187.918 ops/s` | `+0.44%` | `neutral` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed` | `204935.041 ops/s` | `200918.194 ops/s` | `-1.96%` | `neutral` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:getWorkload` | `202333.829 ops/s` | `198679.987 ops/s` | `-1.81%` | `neutral` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:putWorkload` | `2601.212 ops/s` | `2238.208 ops/s` | `-13.96%` | `worse` |
| `segment-index-persisted-mutation:deleteAsyncJoin` | `2128.390 ops/s` | `2270.552 ops/s` | `+6.68%` | `better` |
| `segment-index-persisted-mutation:deleteSync` | `2324.270 ops/s` | `2520.489 ops/s` | `+8.44%` | `better` |
| `segment-index-persisted-mutation:putAsyncJoin` | `2044.173 ops/s` | `2280.671 ops/s` | `+11.57%` | `better` |
| `segment-index-persisted-mutation:putSync` | `2235.482 ops/s` | `2517.162 ops/s` | `+12.60%` | `better` |
