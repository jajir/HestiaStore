# Benchmark Comparison

- Candidate labels rerun 3x total and median-merged because the initial regression exceeded threshold with high relative score error: `segment-index-get-multisegment-cold`


- Profile: `segment-index-nightly`
- Baseline SHA: `a87e6e844c0f83bfc93f8ff3748153d6ffec3dbb`
- Candidate SHA: `e72f4974ea597aa48d2725b131d13e39bcfa6872`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `segment-index-get-multisegment-cold:getHitAsyncJoin` | `41.848 ops/s` | `41.863 ops/s` | `+0.04%` | `neutral` |
| `segment-index-get-multisegment-cold:getHitSync` | `37.106 ops/s` | `42.981 ops/s` | `+15.83%` | `better` |
| `segment-index-get-multisegment-cold:getMissAsyncJoin` | `184141.071 ops/s` | `184735.600 ops/s` | `+0.32%` | `neutral` |
| `segment-index-get-multisegment-cold:getMissSync` | `3524762.741 ops/s` | `3610324.610 ops/s` | `+2.43%` | `neutral` |
| `segment-index-get-multisegment-hot:getHitAsyncJoin` | `77.687 ops/s` | `104.175 ops/s` | `+34.09%` | `better` |
| `segment-index-get-multisegment-hot:getHitSync` | `85.337 ops/s` | `83.324 ops/s` | `-2.36%` | `neutral` |
| `segment-index-get-multisegment-hot:getMissAsyncJoin` | `185861.016 ops/s` | `183575.570 ops/s` | `-1.23%` | `neutral` |
| `segment-index-get-multisegment-hot:getMissSync` | `3713360.080 ops/s` | `3614657.196 ops/s` | `-2.66%` | `neutral` |
| `segment-index-get-overlay:getHitAsyncJoin` | `184276.094 ops/s` | `178749.820 ops/s` | `-3.00%` | `neutral` |
| `segment-index-get-overlay:getHitSync` | `3596877.417 ops/s` | `3646159.138 ops/s` | `+1.37%` | `neutral` |
| `segment-index-get-overlay:getMissAsyncJoin` | `178269.445 ops/s` | `184503.765 ops/s` | `+3.50%` | `better` |
| `segment-index-get-overlay:getMissSync` | `3626601.223 ops/s` | `3607900.995 ops/s` | `-0.52%` | `neutral` |
| `segment-index-get-persisted:getHitAsyncJoin` | `63433.924 ops/s` | `63460.522 ops/s` | `+0.04%` | `neutral` |
| `segment-index-get-persisted:getHitSync` | `111348.489 ops/s` | `115531.183 ops/s` | `+3.76%` | `better` |
| `segment-index-get-persisted:getMissAsyncJoin` | `182148.833 ops/s` | `184227.258 ops/s` | `+1.14%` | `neutral` |
| `segment-index-get-persisted:getMissSync` | `3578797.011 ops/s` | `3492257.946 ops/s` | `-2.42%` | `neutral` |
| `segment-index-hot-partition-put:putHotPartition` | `2892627.254 ops/s` | `2514225.996 ops/s` | `-13.08%` | `worse` |
| `segment-index-hot-partition-put:putThenGetHotPartition` | `1390059.799 ops/s` | `1541006.836 ops/s` | `+10.86%` | `better` |
| `segment-index-lifecycle:openAndCheckAndRepairConsistency` | `279.478 ms/op` | `277.935 ms/op` | `-0.55%` | `neutral` |
| `segment-index-lifecycle:openAndCompact` | `300.764 ms/op` | `303.057 ms/op` | `+0.76%` | `neutral` |
| `segment-index-lifecycle:openExisting` | `273.179 ms/op` | `274.227 ms/op` | `+0.38%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed` | `496331.390 ops/s` | `506383.677 ops/s` | `+2.03%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed:getWorkload` | `491001.429 ops/s` | `501022.709 ops/s` | `+2.04%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed:putWorkload` | `5329.962 ops/s` | `5360.969 ops/s` | `+0.58%` | `neutral` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed` | `262187.371 ops/s` | `258172.425 ops/s` | `-1.53%` | `neutral` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:getWorkload` | `260703.820 ops/s` | `256720.898 ops/s` | `-1.53%` | `neutral` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:putWorkload` | `1483.551 ops/s` | `1451.527 ops/s` | `-2.16%` | `neutral` |
| `segment-index-persisted-mutation:deleteAsyncJoin` | `2499.897 ops/s` | `2468.930 ops/s` | `-1.24%` | `neutral` |
| `segment-index-persisted-mutation:deleteSync` | `2711.777 ops/s` | `2689.194 ops/s` | `-0.83%` | `neutral` |
| `segment-index-persisted-mutation:putAsyncJoin` | `2424.199 ops/s` | `2410.955 ops/s` | `-0.55%` | `neutral` |
| `segment-index-persisted-mutation:putSync` | `2612.192 ops/s` | `2633.067 ops/s` | `+0.80%` | `neutral` |
| `single-chunk-entry-write-compact:writeEntrySteadyState` | `8106816.847 ops/s` | `8304714.073 ops/s` | `+2.44%` | `neutral` |
| `single-chunk-entry-write-large:writeEntrySteadyState` | `7758417.193 ops/s` | `7527495.602 ops/s` | `-2.98%` | `neutral` |
| `sorted-data-diff-key-read-compact:readNextKey` | `8571221.202 ops/s` | `8477335.790 ops/s` | `-1.10%` | `neutral` |
| `sorted-data-diff-key-read-large:readNextKey` | `6378755.989 ops/s` | `6672990.998 ops/s` | `+4.61%` | `better` |
