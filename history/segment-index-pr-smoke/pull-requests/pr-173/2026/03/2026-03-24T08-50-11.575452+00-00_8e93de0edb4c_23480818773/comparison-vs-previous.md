# Benchmark Comparison

- Candidate labels rerun 3x total and median-merged because the initial regression exceeded threshold with high relative score error: `segment-index-persisted-mutation`


- Profile: `segment-index-pr-smoke`
- Baseline SHA: `9d5a339a22a9b41105834c8ae9ce3405bf76481c`
- Candidate SHA: `8e93de0edb4cf85b6be304f6d6ddd4df7921d6fb`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `segment-index-get-multisegment-hot:getHitAsyncJoin` | `87.887 ops/s` | `101.595 ops/s` | `+15.60%` | `better` |
| `segment-index-get-multisegment-hot:getHitSync` | `90.182 ops/s` | `92.561 ops/s` | `+2.64%` | `neutral` |
| `segment-index-get-multisegment-hot:getMissAsyncJoin` | `165003.911 ops/s` | `177112.986 ops/s` | `+7.34%` | `better` |
| `segment-index-get-multisegment-hot:getMissSync` | `3842175.204 ops/s` | `3980615.073 ops/s` | `+3.60%` | `better` |
| `segment-index-get-overlay:getHitAsyncJoin` | `156383.765 ops/s` | `173373.326 ops/s` | `+10.86%` | `better` |
| `segment-index-get-overlay:getHitSync` | `4242771.883 ops/s` | `4350274.442 ops/s` | `+2.53%` | `neutral` |
| `segment-index-get-overlay:getMissAsyncJoin` | `158384.833 ops/s` | `173797.462 ops/s` | `+9.73%` | `better` |
| `segment-index-get-overlay:getMissSync` | `3927878.358 ops/s` | `4019173.440 ops/s` | `+2.32%` | `neutral` |
| `segment-index-get-persisted:getHitAsyncJoin` | `56455.848 ops/s` | `59935.247 ops/s` | `+6.16%` | `better` |
| `segment-index-get-persisted:getHitSync` | `106054.215 ops/s` | `100673.031 ops/s` | `-5.07%` | `warning` |
| `segment-index-get-persisted:getMissAsyncJoin` | `163018.136 ops/s` | `177449.010 ops/s` | `+8.85%` | `better` |
| `segment-index-get-persisted:getMissSync` | `3859341.319 ops/s` | `3898126.970 ops/s` | `+1.00%` | `neutral` |
| `segment-index-hot-partition-put:putHotPartition` | `3028321.041 ops/s` | `2953787.637 ops/s` | `-2.46%` | `neutral` |
| `segment-index-hot-partition-put:putThenGetHotPartition` | `1664173.925 ops/s` | `1682900.071 ops/s` | `+1.13%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed` | `407462.667 ops/s` | `436615.661 ops/s` | `+7.15%` | `better` |
| `segment-index-mixed-drain:partitionedIngestMixed:getWorkload` | `402274.749 ops/s` | `431467.398 ops/s` | `+7.26%` | `better` |
| `segment-index-mixed-drain:partitionedIngestMixed:putWorkload` | `5187.918 ops/s` | `5148.262 ops/s` | `-0.76%` | `neutral` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed` | `200918.194 ops/s` | `276787.208 ops/s` | `+37.76%` | `better` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:getWorkload` | `198679.987 ops/s` | `203289.906 ops/s` | `+2.32%` | `neutral` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:putWorkload` | `2238.208 ops/s` | `73497.302 ops/s` | `+3183.76%` | `better` |
| `segment-index-persisted-mutation:deleteAsyncJoin` | `2270.552 ops/s` | `1259.265 ops/s` | `-44.54%` | `worse` |
| `segment-index-persisted-mutation:deleteSync` | `2520.489 ops/s` | `1486.342 ops/s` | `-41.03%` | `worse` |
| `segment-index-persisted-mutation:putAsyncJoin` | `2280.671 ops/s` | `1242.005 ops/s` | `-45.54%` | `worse` |
| `segment-index-persisted-mutation:putSync` | `2517.162 ops/s` | `1326.345 ops/s` | `-47.31%` | `worse` |
