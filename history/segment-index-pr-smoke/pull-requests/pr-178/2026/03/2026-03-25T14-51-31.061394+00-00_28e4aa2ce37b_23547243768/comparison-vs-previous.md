# Benchmark Comparison

- Candidate labels rerun 3x total and median-merged because the initial regression exceeded threshold with high relative score error: `segment-index-get-multisegment-hot,segment-index-get-overlay,segment-index-get-persisted,segment-index-hot-partition-put`


- Profile: `segment-index-pr-smoke`
- Baseline SHA: `9d5a339a22a9b41105834c8ae9ce3405bf76481c`
- Candidate SHA: `28e4aa2ce37b3eddceb86d33a4379b8b3cf9fe93`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `segment-index-get-multisegment-hot:getHitAsyncJoin` | `87.887 ops/s` | `82.506 ops/s` | `-6.12%` | `warning` |
| `segment-index-get-multisegment-hot:getHitSync` | `90.182 ops/s` | `92.069 ops/s` | `+2.09%` | `neutral` |
| `segment-index-get-multisegment-hot:getMissAsyncJoin` | `165003.911 ops/s` | `236986.245 ops/s` | `+43.62%` | `better` |
| `segment-index-get-multisegment-hot:getMissSync` | `3842175.204 ops/s` | `2483199.524 ops/s` | `-35.37%` | `worse` |
| `segment-index-get-overlay:getHitAsyncJoin` | `156383.765 ops/s` | `234923.414 ops/s` | `+50.22%` | `better` |
| `segment-index-get-overlay:getHitSync` | `4242771.883 ops/s` | `2953894.913 ops/s` | `-30.38%` | `worse` |
| `segment-index-get-overlay:getMissAsyncJoin` | `158384.833 ops/s` | `234392.772 ops/s` | `+47.99%` | `better` |
| `segment-index-get-overlay:getMissSync` | `3927878.358 ops/s` | `2487951.999 ops/s` | `-36.66%` | `worse` |
| `segment-index-get-persisted:getHitAsyncJoin` | `56455.848 ops/s` | `59685.389 ops/s` | `+5.72%` | `better` |
| `segment-index-get-persisted:getHitSync` | `106054.215 ops/s` | `103909.791 ops/s` | `-2.02%` | `neutral` |
| `segment-index-get-persisted:getMissAsyncJoin` | `163018.136 ops/s` | `229634.311 ops/s` | `+40.86%` | `better` |
| `segment-index-get-persisted:getMissSync` | `3859341.319 ops/s` | `2504220.757 ops/s` | `-35.11%` | `worse` |
| `segment-index-hot-partition-put:putHotPartition` | `3028321.041 ops/s` | `2062158.065 ops/s` | `-31.90%` | `worse` |
| `segment-index-hot-partition-put:putThenGetHotPartition` | `1664173.925 ops/s` | `1142451.665 ops/s` | `-31.35%` | `worse` |
| `segment-index-mixed-drain:partitionedIngestMixed` | `407462.667 ops/s` | `386713.021 ops/s` | `-5.09%` | `warning` |
| `segment-index-mixed-drain:partitionedIngestMixed:getWorkload` | `402274.749 ops/s` | `381470.158 ops/s` | `-5.17%` | `warning` |
| `segment-index-mixed-drain:partitionedIngestMixed:putWorkload` | `5187.918 ops/s` | `5242.863 ops/s` | `+1.06%` | `neutral` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed` | `200918.194 ops/s` | `187991.165 ops/s` | `-6.43%` | `warning` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:getWorkload` | `198679.987 ops/s` | `185436.006 ops/s` | `-6.67%` | `warning` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:putWorkload` | `2238.208 ops/s` | `2555.158 ops/s` | `+14.16%` | `better` |
| `segment-index-persisted-mutation:deleteAsyncJoin` | `2270.552 ops/s` | `2918.136 ops/s` | `+28.52%` | `better` |
| `segment-index-persisted-mutation:deleteSync` | `2520.489 ops/s` | `3310.981 ops/s` | `+31.36%` | `better` |
| `segment-index-persisted-mutation:putAsyncJoin` | `2280.671 ops/s` | `2954.481 ops/s` | `+29.54%` | `better` |
| `segment-index-persisted-mutation:putSync` | `2517.162 ops/s` | `3267.978 ops/s` | `+29.83%` | `better` |
