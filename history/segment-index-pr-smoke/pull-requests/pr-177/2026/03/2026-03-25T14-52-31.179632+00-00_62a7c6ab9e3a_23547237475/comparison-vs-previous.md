# Benchmark Comparison

- Candidate labels rerun 3x total and median-merged because the initial regression exceeded threshold with high relative score error: `segment-index-get-persisted,segment-index-persisted-mutation`


- Profile: `segment-index-pr-smoke`
- Baseline SHA: `9d5a339a22a9b41105834c8ae9ce3405bf76481c`
- Candidate SHA: `62a7c6ab9e3a7e656a2557cb25ec138db228dc91`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `segment-index-get-multisegment-hot:getHitAsyncJoin` | `87.887 ops/s` | `85.969 ops/s` | `-2.18%` | `neutral` |
| `segment-index-get-multisegment-hot:getHitSync` | `90.182 ops/s` | `97.048 ops/s` | `+7.61%` | `better` |
| `segment-index-get-multisegment-hot:getMissAsyncJoin` | `165003.911 ops/s` | `186299.087 ops/s` | `+12.91%` | `better` |
| `segment-index-get-multisegment-hot:getMissSync` | `3842175.204 ops/s` | `3681192.594 ops/s` | `-4.19%` | `warning` |
| `segment-index-get-overlay:getHitAsyncJoin` | `156383.765 ops/s` | `167851.656 ops/s` | `+7.33%` | `better` |
| `segment-index-get-overlay:getHitSync` | `4242771.883 ops/s` | `4048282.206 ops/s` | `-4.58%` | `warning` |
| `segment-index-get-overlay:getMissAsyncJoin` | `158384.833 ops/s` | `177324.624 ops/s` | `+11.96%` | `better` |
| `segment-index-get-overlay:getMissSync` | `3927878.358 ops/s` | `3836157.997 ops/s` | `-2.34%` | `neutral` |
| `segment-index-get-persisted:getHitAsyncJoin` | `56455.848 ops/s` | `58065.677 ops/s` | `+2.85%` | `neutral` |
| `segment-index-get-persisted:getHitSync` | `106054.215 ops/s` | `107076.166 ops/s` | `+0.96%` | `neutral` |
| `segment-index-get-persisted:getMissAsyncJoin` | `163018.136 ops/s` | `173680.735 ops/s` | `+6.54%` | `better` |
| `segment-index-get-persisted:getMissSync` | `3859341.319 ops/s` | `4082888.242 ops/s` | `+5.79%` | `better` |
| `segment-index-hot-partition-put:putHotPartition` | `3028321.041 ops/s` | `2928411.817 ops/s` | `-3.30%` | `warning` |
| `segment-index-hot-partition-put:putThenGetHotPartition` | `1664173.925 ops/s` | `1603008.084 ops/s` | `-3.68%` | `warning` |
| `segment-index-mixed-drain:partitionedIngestMixed` | `407462.667 ops/s` | `436580.657 ops/s` | `+7.15%` | `better` |
| `segment-index-mixed-drain:partitionedIngestMixed:getWorkload` | `402274.749 ops/s` | `431366.173 ops/s` | `+7.23%` | `better` |
| `segment-index-mixed-drain:partitionedIngestMixed:putWorkload` | `5187.918 ops/s` | `5214.484 ops/s` | `+0.51%` | `neutral` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed` | `200918.194 ops/s` | `193422.476 ops/s` | `-3.73%` | `warning` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:getWorkload` | `198679.987 ops/s` | `190782.003 ops/s` | `-3.98%` | `warning` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:putWorkload` | `2238.208 ops/s` | `2640.473 ops/s` | `+17.97%` | `better` |
| `segment-index-persisted-mutation:deleteAsyncJoin` | `2270.552 ops/s` | `1804.201 ops/s` | `-20.54%` | `worse` |
| `segment-index-persisted-mutation:deleteSync` | `2520.489 ops/s` | `1945.394 ops/s` | `-22.82%` | `worse` |
| `segment-index-persisted-mutation:putAsyncJoin` | `2280.671 ops/s` | `1731.352 ops/s` | `-24.09%` | `worse` |
| `segment-index-persisted-mutation:putSync` | `2517.162 ops/s` | `1871.563 ops/s` | `-25.65%` | `worse` |
