# Benchmark Comparison

- Profile: `segment-index-pr-smoke`
- Baseline SHA: `9d5a339a22a9b41105834c8ae9ce3405bf76481c`
- Candidate SHA: `819560bb68e5b08c22a1c31c8352bdb544f874d6`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `segment-index-get-multisegment-hot:getHitAsyncJoin` | `87.887 ops/s` | `96.471 ops/s` | `+9.77%` | `better` |
| `segment-index-get-multisegment-hot:getHitSync` | `90.182 ops/s` | `93.974 ops/s` | `+4.20%` | `better` |
| `segment-index-get-multisegment-hot:getMissAsyncJoin` | `165003.911 ops/s` | `165829.631 ops/s` | `+0.50%` | `neutral` |
| `segment-index-get-multisegment-hot:getMissSync` | `3842175.204 ops/s` | `3977153.879 ops/s` | `+3.51%` | `better` |
| `segment-index-get-overlay:getHitAsyncJoin` | `156383.765 ops/s` | `162709.483 ops/s` | `+4.04%` | `better` |
| `segment-index-get-overlay:getHitSync` | `4242771.883 ops/s` | `4306516.481 ops/s` | `+1.50%` | `neutral` |
| `segment-index-get-overlay:getMissAsyncJoin` | `158384.833 ops/s` | `165304.920 ops/s` | `+4.37%` | `better` |
| `segment-index-get-overlay:getMissSync` | `3927878.358 ops/s` | `4041421.435 ops/s` | `+2.89%` | `neutral` |
| `segment-index-get-persisted:getHitAsyncJoin` | `56455.848 ops/s` | `55945.517 ops/s` | `-0.90%` | `neutral` |
| `segment-index-get-persisted:getHitSync` | `106054.215 ops/s` | `100178.060 ops/s` | `-5.54%` | `warning` |
| `segment-index-get-persisted:getMissAsyncJoin` | `163018.136 ops/s` | `156980.173 ops/s` | `-3.70%` | `warning` |
| `segment-index-get-persisted:getMissSync` | `3859341.319 ops/s` | `3946078.904 ops/s` | `+2.25%` | `neutral` |
| `segment-index-hot-partition-put:putHotPartition` | `3028321.041 ops/s` | `3060020.802 ops/s` | `+1.05%` | `neutral` |
| `segment-index-hot-partition-put:putThenGetHotPartition` | `1664173.925 ops/s` | `1733016.585 ops/s` | `+4.14%` | `better` |
| `segment-index-mixed-drain:partitionedIngestMixed` | `407462.667 ops/s` | `410988.659 ops/s` | `+0.87%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed:getWorkload` | `402274.749 ops/s` | `405866.273 ops/s` | `+0.89%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed:putWorkload` | `5187.918 ops/s` | `5122.386 ops/s` | `-1.26%` | `neutral` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed` | `200918.194 ops/s` | `194228.940 ops/s` | `-3.33%` | `warning` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:getWorkload` | `198679.987 ops/s` | `191619.646 ops/s` | `-3.55%` | `warning` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:putWorkload` | `2238.208 ops/s` | `2609.295 ops/s` | `+16.58%` | `better` |
| `segment-index-persisted-mutation:deleteAsyncJoin` | `2270.552 ops/s` | `2161.245 ops/s` | `-4.81%` | `warning` |
| `segment-index-persisted-mutation:deleteSync` | `2520.489 ops/s` | `2524.086 ops/s` | `+0.14%` | `neutral` |
| `segment-index-persisted-mutation:putAsyncJoin` | `2280.671 ops/s` | `2186.526 ops/s` | `-4.13%` | `warning` |
| `segment-index-persisted-mutation:putSync` | `2517.162 ops/s` | `2452.981 ops/s` | `-2.55%` | `neutral` |
