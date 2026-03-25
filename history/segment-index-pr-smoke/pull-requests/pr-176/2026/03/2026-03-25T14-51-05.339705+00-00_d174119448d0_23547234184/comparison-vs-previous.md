# Benchmark Comparison

- Candidate labels rerun 3x total and median-merged because the initial regression exceeded threshold with high relative score error: `segment-index-get-multisegment-hot`


- Profile: `segment-index-pr-smoke`
- Baseline SHA: `9d5a339a22a9b41105834c8ae9ce3405bf76481c`
- Candidate SHA: `d174119448d0200b51811c4ad74a6430c08996e6`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `segment-index-get-multisegment-hot:getHitAsyncJoin` | `87.887 ops/s` | `93.005 ops/s` | `+5.82%` | `better` |
| `segment-index-get-multisegment-hot:getHitSync` | `90.182 ops/s` | `85.524 ops/s` | `-5.16%` | `warning` |
| `segment-index-get-multisegment-hot:getMissAsyncJoin` | `165003.911 ops/s` | `165584.552 ops/s` | `+0.35%` | `neutral` |
| `segment-index-get-multisegment-hot:getMissSync` | `3842175.204 ops/s` | `3798724.046 ops/s` | `-1.13%` | `neutral` |
| `segment-index-get-overlay:getHitAsyncJoin` | `156383.765 ops/s` | `156612.738 ops/s` | `+0.15%` | `neutral` |
| `segment-index-get-overlay:getHitSync` | `4242771.883 ops/s` | `4226682.866 ops/s` | `-0.38%` | `neutral` |
| `segment-index-get-overlay:getMissAsyncJoin` | `158384.833 ops/s` | `164850.179 ops/s` | `+4.08%` | `better` |
| `segment-index-get-overlay:getMissSync` | `3927878.358 ops/s` | `3826942.806 ops/s` | `-2.57%` | `neutral` |
| `segment-index-get-persisted:getHitAsyncJoin` | `56455.848 ops/s` | `59751.661 ops/s` | `+5.84%` | `better` |
| `segment-index-get-persisted:getHitSync` | `106054.215 ops/s` | `102211.802 ops/s` | `-3.62%` | `warning` |
| `segment-index-get-persisted:getMissAsyncJoin` | `163018.136 ops/s` | `157730.193 ops/s` | `-3.24%` | `warning` |
| `segment-index-get-persisted:getMissSync` | `3859341.319 ops/s` | `3992509.008 ops/s` | `+3.45%` | `better` |
| `segment-index-hot-partition-put:putHotPartition` | `3028321.041 ops/s` | `2893563.453 ops/s` | `-4.45%` | `warning` |
| `segment-index-hot-partition-put:putThenGetHotPartition` | `1664173.925 ops/s` | `1707751.980 ops/s` | `+2.62%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed` | `407462.667 ops/s` | `440826.833 ops/s` | `+8.19%` | `better` |
| `segment-index-mixed-drain:partitionedIngestMixed:getWorkload` | `402274.749 ops/s` | `435636.137 ops/s` | `+8.29%` | `better` |
| `segment-index-mixed-drain:partitionedIngestMixed:putWorkload` | `5187.918 ops/s` | `5190.696 ops/s` | `+0.05%` | `neutral` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed` | `200918.194 ops/s` | `330632.487 ops/s` | `+64.56%` | `better` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:getWorkload` | `198679.987 ops/s` | `199177.093 ops/s` | `+0.25%` | `neutral` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:putWorkload` | `2238.208 ops/s` | `131455.394 ops/s` | `+5773.24%` | `better` |
| `segment-index-persisted-mutation:deleteAsyncJoin` | `2270.552 ops/s` | `2315.831 ops/s` | `+1.99%` | `neutral` |
| `segment-index-persisted-mutation:deleteSync` | `2520.489 ops/s` | `2627.500 ops/s` | `+4.25%` | `better` |
| `segment-index-persisted-mutation:putAsyncJoin` | `2280.671 ops/s` | `2332.564 ops/s` | `+2.28%` | `neutral` |
| `segment-index-persisted-mutation:putSync` | `2517.162 ops/s` | `2515.378 ops/s` | `-0.07%` | `neutral` |
