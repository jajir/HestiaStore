# Benchmark Comparison

- Candidate labels rerun 3x total and median-merged because the initial regression exceeded threshold with high relative score error: `segment-index-get-multisegment-hot`


- Profile: `segment-index-pr-smoke`
- Baseline SHA: `a87e6e844c0f83bfc93f8ff3748153d6ffec3dbb`
- Candidate SHA: `e72f4974ea597aa48d2725b131d13e39bcfa6872`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `segment-index-get-multisegment-hot:getHitAsyncJoin` | `84.792 ops/s` | `97.043 ops/s` | `+14.45%` | `better` |
| `segment-index-get-multisegment-hot:getHitSync` | `94.134 ops/s` | `84.004 ops/s` | `-10.76%` | `worse` |
| `segment-index-get-multisegment-hot:getMissAsyncJoin` | `165180.641 ops/s` | `165098.087 ops/s` | `-0.05%` | `neutral` |
| `segment-index-get-multisegment-hot:getMissSync` | `3643691.940 ops/s` | `3826025.739 ops/s` | `+5.00%` | `better` |
| `segment-index-get-overlay:getHitAsyncJoin` | `159118.489 ops/s` | `155664.746 ops/s` | `-2.17%` | `neutral` |
| `segment-index-get-overlay:getHitSync` | `4323633.189 ops/s` | `4213435.612 ops/s` | `-2.55%` | `neutral` |
| `segment-index-get-overlay:getMissAsyncJoin` | `164390.147 ops/s` | `156455.503 ops/s` | `-4.83%` | `warning` |
| `segment-index-get-overlay:getMissSync` | `3876967.778 ops/s` | `4124415.193 ops/s` | `+6.38%` | `better` |
| `segment-index-get-persisted:getHitAsyncJoin` | `54210.066 ops/s` | `55789.753 ops/s` | `+2.91%` | `neutral` |
| `segment-index-get-persisted:getHitSync` | `107048.121 ops/s` | `104396.145 ops/s` | `-2.48%` | `neutral` |
| `segment-index-get-persisted:getMissAsyncJoin` | `164494.979 ops/s` | `165994.696 ops/s` | `+0.91%` | `neutral` |
| `segment-index-get-persisted:getMissSync` | `3988713.083 ops/s` | `3940414.940 ops/s` | `-1.21%` | `neutral` |
| `segment-index-hot-partition-put:putHotPartition` | `3168517.519 ops/s` | `3124721.140 ops/s` | `-1.38%` | `neutral` |
| `segment-index-hot-partition-put:putThenGetHotPartition` | `1722313.062 ops/s` | `1717560.089 ops/s` | `-0.28%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed` | `434091.781 ops/s` | `455608.507 ops/s` | `+4.96%` | `better` |
| `segment-index-mixed-drain:partitionedIngestMixed:getWorkload` | `428941.347 ops/s` | `450389.833 ops/s` | `+5.00%` | `better` |
| `segment-index-mixed-drain:partitionedIngestMixed:putWorkload` | `5150.434 ops/s` | `5218.674 ops/s` | `+1.32%` | `neutral` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed` | `196205.539 ops/s` | `207684.917 ops/s` | `+5.85%` | `better` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:getWorkload` | `193517.358 ops/s` | `205044.275 ops/s` | `+5.96%` | `better` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:putWorkload` | `2688.181 ops/s` | `2640.642 ops/s` | `-1.77%` | `neutral` |
| `segment-index-persisted-mutation:deleteAsyncJoin` | `2225.183 ops/s` | `2160.231 ops/s` | `-2.92%` | `neutral` |
| `segment-index-persisted-mutation:deleteSync` | `2489.078 ops/s` | `2479.974 ops/s` | `-0.37%` | `neutral` |
| `segment-index-persisted-mutation:putAsyncJoin` | `2165.020 ops/s` | `2215.411 ops/s` | `+2.33%` | `neutral` |
| `segment-index-persisted-mutation:putSync` | `2523.412 ops/s` | `2514.868 ops/s` | `-0.34%` | `neutral` |
| `single-chunk-entry-write:writeEntrySteadyState` | `7846877.342 ops/s` | `7932313.730 ops/s` | `+1.09%` | `neutral` |
| `sorted-data-diff-key-read:readNextKey` | `6241634.759 ops/s` | `6487103.344 ops/s` | `+3.93%` | `better` |
