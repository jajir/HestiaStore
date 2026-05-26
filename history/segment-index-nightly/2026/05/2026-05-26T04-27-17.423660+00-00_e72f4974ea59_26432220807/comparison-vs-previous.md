# Benchmark Comparison

- Candidate labels rerun 3x total and median-merged because the initial regression exceeded threshold with high relative score error: `segment-index-get-multisegment-cold,segment-index-get-multisegment-hot`


- Profile: `segment-index-nightly`
- Baseline SHA: `e72f4974ea597aa48d2725b131d13e39bcfa6872`
- Candidate SHA: `e72f4974ea597aa48d2725b131d13e39bcfa6872`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `segment-index-get-multisegment-cold:getHitAsyncJoin` | `50.099 ops/s` | `42.903 ops/s` | `-14.36%` | `worse` |
| `segment-index-get-multisegment-cold:getHitSync` | `51.594 ops/s` | `42.652 ops/s` | `-17.33%` | `worse` |
| `segment-index-get-multisegment-cold:getMissAsyncJoin` | `174032.979 ops/s` | `172845.068 ops/s` | `-0.68%` | `neutral` |
| `segment-index-get-multisegment-cold:getMissSync` | `3929469.647 ops/s` | `3572506.758 ops/s` | `-9.08%` | `worse` |
| `segment-index-get-multisegment-hot:getHitAsyncJoin` | `101.865 ops/s` | `100.837 ops/s` | `-1.01%` | `neutral` |
| `segment-index-get-multisegment-hot:getHitSync` | `104.853 ops/s` | `94.840 ops/s` | `-9.55%` | `worse` |
| `segment-index-get-multisegment-hot:getMissAsyncJoin` | `173027.347 ops/s` | `171831.339 ops/s` | `-0.69%` | `neutral` |
| `segment-index-get-multisegment-hot:getMissSync` | `3754841.771 ops/s` | `3671588.894 ops/s` | `-2.22%` | `neutral` |
| `segment-index-get-overlay:getHitAsyncJoin` | `163623.649 ops/s` | `164026.828 ops/s` | `+0.25%` | `neutral` |
| `segment-index-get-overlay:getHitSync` | `3936716.745 ops/s` | `3882969.258 ops/s` | `-1.37%` | `neutral` |
| `segment-index-get-overlay:getMissAsyncJoin` | `158946.402 ops/s` | `159826.707 ops/s` | `+0.55%` | `neutral` |
| `segment-index-get-overlay:getMissSync` | `3670571.520 ops/s` | `4056121.306 ops/s` | `+10.50%` | `better` |
| `segment-index-get-persisted:getHitAsyncJoin` | `61893.187 ops/s` | `58696.861 ops/s` | `-5.16%` | `warning` |
| `segment-index-get-persisted:getHitSync` | `118696.034 ops/s` | `114910.740 ops/s` | `-3.19%` | `warning` |
| `segment-index-get-persisted:getMissAsyncJoin` | `156265.841 ops/s` | `168937.442 ops/s` | `+8.11%` | `better` |
| `segment-index-get-persisted:getMissSync` | `3825206.838 ops/s` | `3879551.368 ops/s` | `+1.42%` | `neutral` |
| `segment-index-hot-partition-put:putHotPartition` | `3366835.139 ops/s` | `2846695.185 ops/s` | `-15.45%` | `worse` |
| `segment-index-hot-partition-put:putThenGetHotPartition` | `1619821.912 ops/s` | `1525356.259 ops/s` | `-5.83%` | `warning` |
| `segment-index-lifecycle:openAndCheckAndRepairConsistency` | `242.813 ms/op` | `253.900 ms/op` | `+4.57%` | `better` |
| `segment-index-lifecycle:openAndCompact` | `266.432 ms/op` | `270.969 ms/op` | `+1.70%` | `neutral` |
| `segment-index-lifecycle:openExisting` | `241.790 ms/op` | `246.288 ms/op` | `+1.86%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed` | `531987.724 ops/s` | `518811.323 ops/s` | `-2.48%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed:getWorkload` | `526659.263 ops/s` | `513518.529 ops/s` | `-2.50%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed:putWorkload` | `5328.461 ops/s` | `5292.794 ops/s` | `-0.67%` | `neutral` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed` | `271456.521 ops/s` | `260908.579 ops/s` | `-3.89%` | `warning` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:getWorkload` | `269907.618 ops/s` | `259281.456 ops/s` | `-3.94%` | `warning` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:putWorkload` | `1548.903 ops/s` | `1627.123 ops/s` | `+5.05%` | `better` |
| `segment-index-persisted-mutation:deleteAsyncJoin` | `2090.564 ops/s` | `2021.077 ops/s` | `-3.32%` | `warning` |
| `segment-index-persisted-mutation:deleteSync` | `2292.814 ops/s` | `2262.882 ops/s` | `-1.31%` | `neutral` |
| `segment-index-persisted-mutation:putAsyncJoin` | `2068.492 ops/s` | `1995.631 ops/s` | `-3.52%` | `warning` |
| `segment-index-persisted-mutation:putSync` | `2283.469 ops/s` | `2208.554 ops/s` | `-3.28%` | `warning` |
| `single-chunk-entry-write-compact:writeEntrySteadyState` | `8330173.938 ops/s` | `8506146.537 ops/s` | `+2.11%` | `neutral` |
| `single-chunk-entry-write-large:writeEntrySteadyState` | `7918703.491 ops/s` | `7919612.395 ops/s` | `+0.01%` | `neutral` |
| `sorted-data-diff-key-read-compact:readNextKey` | `9065127.756 ops/s` | `8627218.634 ops/s` | `-4.83%` | `warning` |
| `sorted-data-diff-key-read-large:readNextKey` | `7640338.109 ops/s` | `7048574.773 ops/s` | `-7.75%` | `worse` |
