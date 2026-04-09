# Benchmark Comparison

- Candidate labels rerun 3x total and median-merged because the initial regression exceeded threshold with high relative score error: `segment-index-lifecycle`


- Profile: `segment-index-nightly`
- Baseline SHA: `e72f4974ea597aa48d2725b131d13e39bcfa6872`
- Candidate SHA: `e72f4974ea597aa48d2725b131d13e39bcfa6872`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `segment-index-get-multisegment-cold:getHitAsyncJoin` | `41.510 ops/s` | `50.124 ops/s` | `+20.75%` | `better` |
| `segment-index-get-multisegment-cold:getHitSync` | `42.193 ops/s` | `41.836 ops/s` | `-0.85%` | `neutral` |
| `segment-index-get-multisegment-cold:getMissAsyncJoin` | `188375.065 ops/s` | `171643.616 ops/s` | `-8.88%` | `worse` |
| `segment-index-get-multisegment-cold:getMissSync` | `3754666.548 ops/s` | `4507423.675 ops/s` | `+20.05%` | `better` |
| `segment-index-get-multisegment-hot:getHitAsyncJoin` | `90.250 ops/s` | `96.289 ops/s` | `+6.69%` | `better` |
| `segment-index-get-multisegment-hot:getHitSync` | `93.970 ops/s` | `92.278 ops/s` | `-1.80%` | `neutral` |
| `segment-index-get-multisegment-hot:getMissAsyncJoin` | `187023.860 ops/s` | `172310.621 ops/s` | `-7.87%` | `worse` |
| `segment-index-get-multisegment-hot:getMissSync` | `3793407.064 ops/s` | `3753311.780 ops/s` | `-1.06%` | `neutral` |
| `segment-index-get-overlay:getHitAsyncJoin` | `181472.102 ops/s` | `164775.014 ops/s` | `-9.20%` | `worse` |
| `segment-index-get-overlay:getHitSync` | `3800161.125 ops/s` | `4070241.270 ops/s` | `+7.11%` | `better` |
| `segment-index-get-overlay:getMissAsyncJoin` | `182191.095 ops/s` | `157176.791 ops/s` | `-13.73%` | `worse` |
| `segment-index-get-overlay:getMissSync` | `3673943.070 ops/s` | `3505853.562 ops/s` | `-4.58%` | `warning` |
| `segment-index-get-persisted:getHitAsyncJoin` | `62487.712 ops/s` | `63957.640 ops/s` | `+2.35%` | `neutral` |
| `segment-index-get-persisted:getHitSync` | `116794.489 ops/s` | `114137.974 ops/s` | `-2.27%` | `neutral` |
| `segment-index-get-persisted:getMissAsyncJoin` | `186718.861 ops/s` | `164489.667 ops/s` | `-11.91%` | `worse` |
| `segment-index-get-persisted:getMissSync` | `3570255.221 ops/s` | `3581188.277 ops/s` | `+0.31%` | `neutral` |
| `segment-index-hot-partition-put:putHotPartition` | `2975609.285 ops/s` | `3141985.207 ops/s` | `+5.59%` | `better` |
| `segment-index-hot-partition-put:putThenGetHotPartition` | `1468978.928 ops/s` | `1715208.301 ops/s` | `+16.76%` | `better` |
| `segment-index-lifecycle:openAndCheckAndRepairConsistency` | `277.274 ms/op` | `245.014 ms/op` | `-11.63%` | `worse` |
| `segment-index-lifecycle:openAndCompact` | `302.184 ms/op` | `266.683 ms/op` | `-11.75%` | `worse` |
| `segment-index-lifecycle:openExisting` | `275.306 ms/op` | `242.128 ms/op` | `-12.05%` | `worse` |
| `segment-index-mixed-drain:partitionedIngestMixed` | `502416.169 ops/s` | `535582.626 ops/s` | `+6.60%` | `better` |
| `segment-index-mixed-drain:partitionedIngestMixed:getWorkload` | `497053.501 ops/s` | `530279.545 ops/s` | `+6.68%` | `better` |
| `segment-index-mixed-drain:partitionedIngestMixed:putWorkload` | `5362.668 ops/s` | `5303.081 ops/s` | `-1.11%` | `neutral` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed` | `252828.567 ops/s` | `294092.572 ops/s` | `+16.32%` | `better` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:getWorkload` | `251201.862 ops/s` | `259843.112 ops/s` | `+3.44%` | `better` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:putWorkload` | `1626.705 ops/s` | `34249.460 ops/s` | `+2005.45%` | `better` |
| `segment-index-persisted-mutation:deleteAsyncJoin` | `2558.940 ops/s` | `1986.574 ops/s` | `-22.37%` | `worse` |
| `segment-index-persisted-mutation:deleteSync` | `2784.138 ops/s` | `2158.527 ops/s` | `-22.47%` | `worse` |
| `segment-index-persisted-mutation:putAsyncJoin` | `2499.056 ops/s` | `1952.973 ops/s` | `-21.85%` | `worse` |
| `segment-index-persisted-mutation:putSync` | `2762.131 ops/s` | `2144.307 ops/s` | `-22.37%` | `worse` |
| `single-chunk-entry-write-compact:writeEntrySteadyState` | `8286939.118 ops/s` | `8516521.893 ops/s` | `+2.77%` | `neutral` |
| `single-chunk-entry-write-large:writeEntrySteadyState` | `7777503.167 ops/s` | `7964969.448 ops/s` | `+2.41%` | `neutral` |
| `sorted-data-diff-key-read-compact:readNextKey` | `7805089.899 ops/s` | `9489752.813 ops/s` | `+21.58%` | `better` |
| `sorted-data-diff-key-read-large:readNextKey` | `6749987.496 ops/s` | `7367750.596 ops/s` | `+9.15%` | `better` |
