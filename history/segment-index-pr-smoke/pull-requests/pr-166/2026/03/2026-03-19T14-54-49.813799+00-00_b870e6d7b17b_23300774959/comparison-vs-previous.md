# Benchmark Comparison

- Profile: `segment-index-pr-smoke`
- Baseline SHA: `6885ee00040024ba5bc2fc1f0365778414d7b12d`
- Candidate SHA: `b870e6d7b17b4a13ca326725e7d2232b0fd08daf`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `segment-index-get-multisegment-hot:getHitAsyncJoin` | `85.605 ops/s` | `100.078 ops/s` | `+16.91%` | `better` |
| `segment-index-get-multisegment-hot:getHitSync` | `88.218 ops/s` | `91.134 ops/s` | `+3.31%` | `better` |
| `segment-index-get-multisegment-hot:getMissAsyncJoin` | `176532.156 ops/s` | `177275.488 ops/s` | `+0.42%` | `neutral` |
| `segment-index-get-multisegment-hot:getMissSync` | `3656084.301 ops/s` | `3933750.566 ops/s` | `+7.59%` | `better` |
| `segment-index-get-overlay:getHitAsyncJoin` | `172204.390 ops/s` | `174825.559 ops/s` | `+1.52%` | `neutral` |
| `segment-index-get-overlay:getHitSync` | `4235376.263 ops/s` | `4180592.056 ops/s` | `-1.29%` | `neutral` |
| `segment-index-get-overlay:getMissAsyncJoin` | `175914.469 ops/s` | `171773.897 ops/s` | `-2.35%` | `neutral` |
| `segment-index-get-overlay:getMissSync` | `3927865.533 ops/s` | `3898230.906 ops/s` | `-0.75%` | `neutral` |
| `segment-index-get-persisted:getHitAsyncJoin` | `57402.683 ops/s` | `61527.067 ops/s` | `+7.19%` | `better` |
| `segment-index-get-persisted:getHitSync` | `106533.303 ops/s` | `112520.640 ops/s` | `+5.62%` | `better` |
| `segment-index-get-persisted:getMissAsyncJoin` | `170418.838 ops/s` | `179065.146 ops/s` | `+5.07%` | `better` |
| `segment-index-get-persisted:getMissSync` | `4143871.199 ops/s` | `3780596.582 ops/s` | `-8.77%` | `worse` |
| `segment-index-hot-partition-put:putHotPartition` | `3004364.272 ops/s` | `3187841.250 ops/s` | `+6.11%` | `better` |
| `segment-index-hot-partition-put:putThenGetHotPartition` | `1616236.289 ops/s` | `1657962.758 ops/s` | `+2.58%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed` | `428093.031 ops/s` | `425631.353 ops/s` | `-0.58%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed:getWorkload` | `422931.601 ops/s` | `420599.046 ops/s` | `-0.55%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed:putWorkload` | `5161.430 ops/s` | `5032.306 ops/s` | `-2.50%` | `neutral` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed` | `194098.551 ops/s` | `202748.457 ops/s` | `+4.46%` | `better` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:getWorkload` | `191625.222 ops/s` | `200252.781 ops/s` | `+4.50%` | `better` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:putWorkload` | `2473.329 ops/s` | `2495.676 ops/s` | `+0.90%` | `neutral` |
| `segment-index-persisted-mutation:deleteAsyncJoin` | `1679.848 ops/s` | `1694.655 ops/s` | `+0.88%` | `neutral` |
| `segment-index-persisted-mutation:deleteSync` | `2073.350 ops/s` | `1813.862 ops/s` | `-12.52%` | `worse` |
| `segment-index-persisted-mutation:putAsyncJoin` | `1698.536 ops/s` | `1755.934 ops/s` | `+3.38%` | `better` |
| `segment-index-persisted-mutation:putSync` | `1780.942 ops/s` | `1789.033 ops/s` | `+0.45%` | `neutral` |
