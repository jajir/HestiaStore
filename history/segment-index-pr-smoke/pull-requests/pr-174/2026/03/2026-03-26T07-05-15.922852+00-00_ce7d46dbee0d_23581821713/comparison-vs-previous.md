# Benchmark Comparison

- Candidate labels rerun 3x total and median-merged because the initial regression exceeded threshold with high relative score error: `segment-index-get-multisegment-hot,segment-index-get-persisted,segment-index-hot-partition-put,segment-index-persisted-mutation`


- Profile: `segment-index-pr-smoke`
- Baseline SHA: `a87e6e844c0f83bfc93f8ff3748153d6ffec3dbb`
- Candidate SHA: `ce7d46dbee0d8033415cd8e20131efc17c515cf7`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `segment-index-get-multisegment-hot:getHitAsyncJoin` | `84.792 ops/s` | `94.807 ops/s` | `+11.81%` | `better` |
| `segment-index-get-multisegment-hot:getHitSync` | `94.134 ops/s` | `84.141 ops/s` | `-10.62%` | `worse` |
| `segment-index-get-multisegment-hot:getMissAsyncJoin` | `165180.641 ops/s` | `176275.959 ops/s` | `+6.72%` | `better` |
| `segment-index-get-multisegment-hot:getMissSync` | `3643691.940 ops/s` | `3696219.167 ops/s` | `+1.44%` | `neutral` |
| `segment-index-get-overlay:getHitAsyncJoin` | `159118.489 ops/s` | `172458.476 ops/s` | `+8.38%` | `better` |
| `segment-index-get-overlay:getHitSync` | `4323633.189 ops/s` | `4265738.353 ops/s` | `-1.34%` | `neutral` |
| `segment-index-get-overlay:getMissAsyncJoin` | `164390.147 ops/s` | `176861.122 ops/s` | `+7.59%` | `better` |
| `segment-index-get-overlay:getMissSync` | `3876967.778 ops/s` | `3814161.648 ops/s` | `-1.62%` | `neutral` |
| `segment-index-get-persisted:getHitAsyncJoin` | `54210.066 ops/s` | `57250.834 ops/s` | `+5.61%` | `better` |
| `segment-index-get-persisted:getHitSync` | `107048.121 ops/s` | `106099.441 ops/s` | `-0.89%` | `neutral` |
| `segment-index-get-persisted:getMissAsyncJoin` | `164494.979 ops/s` | `176554.245 ops/s` | `+7.33%` | `better` |
| `segment-index-get-persisted:getMissSync` | `3988713.083 ops/s` | `3936056.289 ops/s` | `-1.32%` | `neutral` |
| `segment-index-hot-partition-put:putHotPartition` | `3168517.519 ops/s` | `2932010.423 ops/s` | `-7.46%` | `worse` |
| `segment-index-hot-partition-put:putThenGetHotPartition` | `1722313.062 ops/s` | `1656084.208 ops/s` | `-3.85%` | `warning` |
| `segment-index-mixed-drain:partitionedIngestMixed` | `434091.781 ops/s` | `450732.982 ops/s` | `+3.83%` | `better` |
| `segment-index-mixed-drain:partitionedIngestMixed:getWorkload` | `428941.347 ops/s` | `445599.840 ops/s` | `+3.88%` | `better` |
| `segment-index-mixed-drain:partitionedIngestMixed:putWorkload` | `5150.434 ops/s` | `5133.142 ops/s` | `-0.34%` | `neutral` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed` | `196205.539 ops/s` | `184723.324 ops/s` | `-5.85%` | `warning` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:getWorkload` | `193517.358 ops/s` | `182193.913 ops/s` | `-5.85%` | `warning` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:putWorkload` | `2688.181 ops/s` | `2529.411 ops/s` | `-5.91%` | `warning` |
| `segment-index-persisted-mutation:deleteAsyncJoin` | `2225.183 ops/s` | `1694.814 ops/s` | `-23.83%` | `worse` |
| `segment-index-persisted-mutation:deleteSync` | `2489.078 ops/s` | `1851.424 ops/s` | `-25.62%` | `worse` |
| `segment-index-persisted-mutation:putAsyncJoin` | `2165.020 ops/s` | `1894.613 ops/s` | `-12.49%` | `worse` |
| `segment-index-persisted-mutation:putSync` | `2523.412 ops/s` | `1678.369 ops/s` | `-33.49%` | `worse` |
| `single-chunk-entry-write:writeEntrySteadyState` | `7846877.342 ops/s` | `-` | `-` | `removed` |
| `sorted-data-diff-key-read:readNextKey` | `6241634.759 ops/s` | `-` | `-` | `removed` |
