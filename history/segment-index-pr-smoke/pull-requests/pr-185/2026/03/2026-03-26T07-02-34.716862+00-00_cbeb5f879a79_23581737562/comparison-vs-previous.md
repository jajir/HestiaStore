# Benchmark Comparison

- Profile: `segment-index-pr-smoke`
- Baseline SHA: `a87e6e844c0f83bfc93f8ff3748153d6ffec3dbb`
- Candidate SHA: `cbeb5f879a7922e5d417d5e0041883ee781d85cf`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `segment-index-get-multisegment-hot:getHitAsyncJoin` | `84.792 ops/s` | `87.248 ops/s` | `+2.90%` | `neutral` |
| `segment-index-get-multisegment-hot:getHitSync` | `94.134 ops/s` | `87.582 ops/s` | `-6.96%` | `warning` |
| `segment-index-get-multisegment-hot:getMissAsyncJoin` | `165180.641 ops/s` | `237039.590 ops/s` | `+43.50%` | `better` |
| `segment-index-get-multisegment-hot:getMissSync` | `3643691.940 ops/s` | `2532769.479 ops/s` | `-30.49%` | `worse` |
| `segment-index-get-overlay:getHitAsyncJoin` | `159118.489 ops/s` | `224527.865 ops/s` | `+41.11%` | `better` |
| `segment-index-get-overlay:getHitSync` | `4323633.189 ops/s` | `2697160.831 ops/s` | `-37.62%` | `worse` |
| `segment-index-get-overlay:getMissAsyncJoin` | `164390.147 ops/s` | `219118.799 ops/s` | `+33.29%` | `better` |
| `segment-index-get-overlay:getMissSync` | `3876967.778 ops/s` | `2636233.536 ops/s` | `-32.00%` | `worse` |
| `segment-index-get-persisted:getHitAsyncJoin` | `54210.066 ops/s` | `72715.841 ops/s` | `+34.14%` | `better` |
| `segment-index-get-persisted:getHitSync` | `107048.121 ops/s` | `102927.766 ops/s` | `-3.85%` | `warning` |
| `segment-index-get-persisted:getMissAsyncJoin` | `164494.979 ops/s` | `228626.799 ops/s` | `+38.99%` | `better` |
| `segment-index-get-persisted:getMissSync` | `3988713.083 ops/s` | `2489269.332 ops/s` | `-37.59%` | `worse` |
| `segment-index-hot-partition-put:putHotPartition` | `3168517.519 ops/s` | `2117409.794 ops/s` | `-33.17%` | `worse` |
| `segment-index-hot-partition-put:putThenGetHotPartition` | `1722313.062 ops/s` | `1066599.199 ops/s` | `-38.07%` | `worse` |
| `segment-index-mixed-drain:partitionedIngestMixed` | `434091.781 ops/s` | `407025.814 ops/s` | `-6.24%` | `warning` |
| `segment-index-mixed-drain:partitionedIngestMixed:getWorkload` | `428941.347 ops/s` | `401861.332 ops/s` | `-6.31%` | `warning` |
| `segment-index-mixed-drain:partitionedIngestMixed:putWorkload` | `5150.434 ops/s` | `5164.482 ops/s` | `+0.27%` | `neutral` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed` | `196205.539 ops/s` | `179791.249 ops/s` | `-8.37%` | `worse` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:getWorkload` | `193517.358 ops/s` | `177277.975 ops/s` | `-8.39%` | `worse` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:putWorkload` | `2688.181 ops/s` | `2513.273 ops/s` | `-6.51%` | `warning` |
| `segment-index-persisted-mutation:deleteAsyncJoin` | `2225.183 ops/s` | `2961.686 ops/s` | `+33.10%` | `better` |
| `segment-index-persisted-mutation:deleteAsyncJoin:diag_directoryCountDelta` | `0.000 ops/s` | `-` | `-` | `removed` |
| `segment-index-persisted-mutation:deleteAsyncJoin:diag_fileBytesDelta` | `56407.140 ops/s` | `-` | `-` | `removed` |
| `segment-index-persisted-mutation:deleteAsyncJoin:diag_fileCountDelta` | `3.983 ops/s` | `-` | `-` | `removed` |
| `segment-index-persisted-mutation:deleteSync` | `2489.078 ops/s` | `3297.718 ops/s` | `+32.49%` | `better` |
| `segment-index-persisted-mutation:deleteSync:diag_directoryCountDelta` | `0.000 ops/s` | `-` | `-` | `removed` |
| `segment-index-persisted-mutation:deleteSync:diag_fileBytesDelta` | `55907.402 ops/s` | `-` | `-` | `removed` |
| `segment-index-persisted-mutation:deleteSync:diag_fileCountDelta` | `2.981 ops/s` | `-` | `-` | `removed` |
| `segment-index-persisted-mutation:putAsyncJoin` | `2165.020 ops/s` | `2794.365 ops/s` | `+29.07%` | `better` |
| `segment-index-persisted-mutation:putAsyncJoin:diag_directoryCountDelta` | `0.000 ops/s` | `-` | `-` | `removed` |
| `segment-index-persisted-mutation:putAsyncJoin:diag_fileBytesDelta` | `226189.089 ops/s` | `-` | `-` | `removed` |
| `segment-index-persisted-mutation:putAsyncJoin:diag_fileCountDelta` | `3.999 ops/s` | `-` | `-` | `removed` |
| `segment-index-persisted-mutation:putSync` | `2523.412 ops/s` | `3170.736 ops/s` | `+25.65%` | `better` |
| `segment-index-persisted-mutation:putSync:diag_directoryCountDelta` | `0.000 ops/s` | `-` | `-` | `removed` |
| `segment-index-persisted-mutation:putSync:diag_fileBytesDelta` | `259544.204 ops/s` | `-` | `-` | `removed` |
| `segment-index-persisted-mutation:putSync:diag_fileCountDelta` | `2.999 ops/s` | `-` | `-` | `removed` |
| `single-chunk-entry-write:writeEntrySteadyState` | `7846877.342 ops/s` | `-` | `-` | `removed` |
| `sorted-data-diff-key-read:readNextKey` | `6241634.759 ops/s` | `-` | `-` | `removed` |
