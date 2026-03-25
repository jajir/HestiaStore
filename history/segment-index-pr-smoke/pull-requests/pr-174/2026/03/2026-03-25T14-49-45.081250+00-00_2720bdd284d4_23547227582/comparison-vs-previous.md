# Benchmark Comparison

- Candidate labels rerun 3x total and median-merged because the initial regression exceeded threshold with high relative score error: `segment-index-mixed-split-heavy,segment-index-persisted-mutation`


- Profile: `segment-index-pr-smoke`
- Baseline SHA: `9d5a339a22a9b41105834c8ae9ce3405bf76481c`
- Candidate SHA: `2720bdd284d42f3c5e4a70aa02097fd2509dcb94`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `segment-index-get-multisegment-hot:getHitAsyncJoin` | `87.887 ops/s` | `93.562 ops/s` | `+6.46%` | `better` |
| `segment-index-get-multisegment-hot:getHitSync` | `90.182 ops/s` | `93.297 ops/s` | `+3.45%` | `better` |
| `segment-index-get-multisegment-hot:getMissAsyncJoin` | `165003.911 ops/s` | `175616.855 ops/s` | `+6.43%` | `better` |
| `segment-index-get-multisegment-hot:getMissSync` | `3842175.204 ops/s` | `3934474.809 ops/s` | `+2.40%` | `neutral` |
| `segment-index-get-overlay:getHitAsyncJoin` | `156383.765 ops/s` | `170258.293 ops/s` | `+8.87%` | `better` |
| `segment-index-get-overlay:getHitSync` | `4242771.883 ops/s` | `4223945.197 ops/s` | `-0.44%` | `neutral` |
| `segment-index-get-overlay:getMissAsyncJoin` | `158384.833 ops/s` | `170154.432 ops/s` | `+7.43%` | `better` |
| `segment-index-get-overlay:getMissSync` | `3927878.358 ops/s` | `3956938.760 ops/s` | `+0.74%` | `neutral` |
| `segment-index-get-persisted:getHitAsyncJoin` | `56455.848 ops/s` | `54122.697 ops/s` | `-4.13%` | `warning` |
| `segment-index-get-persisted:getHitSync` | `106054.215 ops/s` | `109202.646 ops/s` | `+2.97%` | `neutral` |
| `segment-index-get-persisted:getMissAsyncJoin` | `163018.136 ops/s` | `176866.602 ops/s` | `+8.50%` | `better` |
| `segment-index-get-persisted:getMissSync` | `3859341.319 ops/s` | `3627773.489 ops/s` | `-6.00%` | `warning` |
| `segment-index-hot-partition-put:putHotPartition` | `3028321.041 ops/s` | `3109935.493 ops/s` | `+2.70%` | `neutral` |
| `segment-index-hot-partition-put:putThenGetHotPartition` | `1664173.925 ops/s` | `1725881.983 ops/s` | `+3.71%` | `better` |
| `segment-index-mixed-drain:partitionedIngestMixed` | `407462.667 ops/s` | `414875.676 ops/s` | `+1.82%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed:getWorkload` | `402274.749 ops/s` | `409773.517 ops/s` | `+1.86%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed:putWorkload` | `5187.918 ops/s` | `5102.159 ops/s` | `-1.65%` | `neutral` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed` | `200918.194 ops/s` | `188907.332 ops/s` | `-5.98%` | `warning` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:getWorkload` | `198679.987 ops/s` | `186458.246 ops/s` | `-6.15%` | `warning` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:putWorkload` | `2238.208 ops/s` | `2449.085 ops/s` | `+9.42%` | `better` |
| `segment-index-persisted-mutation:deleteAsyncJoin` | `2270.552 ops/s` | `1828.656 ops/s` | `-19.46%` | `worse` |
| `segment-index-persisted-mutation:deleteSync` | `2520.489 ops/s` | `2074.460 ops/s` | `-17.70%` | `worse` |
| `segment-index-persisted-mutation:putAsyncJoin` | `2280.671 ops/s` | `1849.381 ops/s` | `-18.91%` | `worse` |
| `segment-index-persisted-mutation:putSync` | `2517.162 ops/s` | `2006.449 ops/s` | `-20.29%` | `worse` |
