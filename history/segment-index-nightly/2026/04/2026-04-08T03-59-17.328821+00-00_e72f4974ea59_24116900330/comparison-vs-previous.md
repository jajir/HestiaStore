# Benchmark Comparison

- Candidate labels rerun 3x total and median-merged because the initial regression exceeded threshold with high relative score error: `segment-index-get-multisegment-cold,segment-index-get-multisegment-hot`


- Profile: `segment-index-nightly`
- Baseline SHA: `e72f4974ea597aa48d2725b131d13e39bcfa6872`
- Candidate SHA: `e72f4974ea597aa48d2725b131d13e39bcfa6872`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `segment-index-get-multisegment-cold:getHitAsyncJoin` | `41.227 ops/s` | `41.510 ops/s` | `+0.69%` | `neutral` |
| `segment-index-get-multisegment-cold:getHitSync` | `48.833 ops/s` | `42.193 ops/s` | `-13.60%` | `worse` |
| `segment-index-get-multisegment-cold:getMissAsyncJoin` | `168898.081 ops/s` | `188375.065 ops/s` | `+11.53%` | `better` |
| `segment-index-get-multisegment-cold:getMissSync` | `3732416.378 ops/s` | `3754666.548 ops/s` | `+0.60%` | `neutral` |
| `segment-index-get-multisegment-hot:getHitAsyncJoin` | `110.741 ops/s` | `90.250 ops/s` | `-18.50%` | `worse` |
| `segment-index-get-multisegment-hot:getHitSync` | `89.899 ops/s` | `93.970 ops/s` | `+4.53%` | `better` |
| `segment-index-get-multisegment-hot:getMissAsyncJoin` | `168896.909 ops/s` | `187023.860 ops/s` | `+10.73%` | `better` |
| `segment-index-get-multisegment-hot:getMissSync` | `3737556.372 ops/s` | `3793407.064 ops/s` | `+1.49%` | `neutral` |
| `segment-index-get-overlay:getHitAsyncJoin` | `159576.937 ops/s` | `181472.102 ops/s` | `+13.72%` | `better` |
| `segment-index-get-overlay:getHitSync` | `4008933.426 ops/s` | `3800161.125 ops/s` | `-5.21%` | `warning` |
| `segment-index-get-overlay:getMissAsyncJoin` | `159579.492 ops/s` | `182191.095 ops/s` | `+14.17%` | `better` |
| `segment-index-get-overlay:getMissSync` | `3931700.745 ops/s` | `3673943.070 ops/s` | `-6.56%` | `warning` |
| `segment-index-get-persisted:getHitAsyncJoin` | `63605.561 ops/s` | `62487.712 ops/s` | `-1.76%` | `neutral` |
| `segment-index-get-persisted:getHitSync` | `117233.946 ops/s` | `116794.489 ops/s` | `-0.37%` | `neutral` |
| `segment-index-get-persisted:getMissAsyncJoin` | `162149.450 ops/s` | `186718.861 ops/s` | `+15.15%` | `better` |
| `segment-index-get-persisted:getMissSync` | `3405313.619 ops/s` | `3570255.221 ops/s` | `+4.84%` | `better` |
| `segment-index-hot-partition-put:putHotPartition` | `3086276.349 ops/s` | `2975609.285 ops/s` | `-3.59%` | `warning` |
| `segment-index-hot-partition-put:putThenGetHotPartition` | `1474284.505 ops/s` | `1468978.928 ops/s` | `-0.36%` | `neutral` |
| `segment-index-lifecycle:openAndCheckAndRepairConsistency` | `249.489 ms/op` | `277.274 ms/op` | `+11.14%` | `better` |
| `segment-index-lifecycle:openAndCompact` | `269.947 ms/op` | `302.184 ms/op` | `+11.94%` | `better` |
| `segment-index-lifecycle:openExisting` | `245.312 ms/op` | `275.306 ms/op` | `+12.23%` | `better` |
| `segment-index-mixed-drain:partitionedIngestMixed` | `506613.243 ops/s` | `502416.169 ops/s` | `-0.83%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed:getWorkload` | `501328.529 ops/s` | `497053.501 ops/s` | `-0.85%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed:putWorkload` | `5284.714 ops/s` | `5362.668 ops/s` | `+1.48%` | `neutral` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed` | `260572.296 ops/s` | `252828.567 ops/s` | `-2.97%` | `neutral` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:getWorkload` | `259146.775 ops/s` | `251201.862 ops/s` | `-3.07%` | `warning` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:putWorkload` | `1425.521 ops/s` | `1626.705 ops/s` | `+14.11%` | `better` |
| `segment-index-persisted-mutation:deleteAsyncJoin` | `1989.316 ops/s` | `2558.940 ops/s` | `+28.63%` | `better` |
| `segment-index-persisted-mutation:deleteSync` | `2204.342 ops/s` | `2784.138 ops/s` | `+26.30%` | `better` |
| `segment-index-persisted-mutation:putAsyncJoin` | `1959.368 ops/s` | `2499.056 ops/s` | `+27.54%` | `better` |
| `segment-index-persisted-mutation:putSync` | `2180.103 ops/s` | `2762.131 ops/s` | `+26.70%` | `better` |
| `single-chunk-entry-write-compact:writeEntrySteadyState` | `8519375.377 ops/s` | `8286939.118 ops/s` | `-2.73%` | `neutral` |
| `single-chunk-entry-write-large:writeEntrySteadyState` | `7814387.152 ops/s` | `7777503.167 ops/s` | `-0.47%` | `neutral` |
| `sorted-data-diff-key-read-compact:readNextKey` | `8946622.244 ops/s` | `7805089.899 ops/s` | `-12.76%` | `worse` |
| `sorted-data-diff-key-read-large:readNextKey` | `7400117.839 ops/s` | `6749987.496 ops/s` | `-8.79%` | `worse` |
