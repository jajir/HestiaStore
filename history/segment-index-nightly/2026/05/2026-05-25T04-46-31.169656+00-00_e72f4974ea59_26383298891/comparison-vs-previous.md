# Benchmark Comparison

- Profile: `segment-index-nightly`
- Baseline SHA: `a87e6e844c0f83bfc93f8ff3748153d6ffec3dbb`
- Candidate SHA: `e72f4974ea597aa48d2725b131d13e39bcfa6872`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `segment-index-get-multisegment-cold:getHitAsyncJoin` | `44.322 ops/s` | `50.099 ops/s` | `+13.03%` | `better` |
| `segment-index-get-multisegment-cold:getHitSync` | `41.309 ops/s` | `51.594 ops/s` | `+24.90%` | `better` |
| `segment-index-get-multisegment-cold:getMissAsyncJoin` | `173297.460 ops/s` | `174032.979 ops/s` | `+0.42%` | `neutral` |
| `segment-index-get-multisegment-cold:getMissSync` | `4062159.099 ops/s` | `3929469.647 ops/s` | `-3.27%` | `warning` |
| `segment-index-get-multisegment-hot:getHitAsyncJoin` | `97.554 ops/s` | `101.865 ops/s` | `+4.42%` | `better` |
| `segment-index-get-multisegment-hot:getHitSync` | `76.214 ops/s` | `104.853 ops/s` | `+37.58%` | `better` |
| `segment-index-get-multisegment-hot:getMissAsyncJoin` | `174709.718 ops/s` | `173027.347 ops/s` | `-0.96%` | `neutral` |
| `segment-index-get-multisegment-hot:getMissSync` | `4105817.008 ops/s` | `3754841.771 ops/s` | `-8.55%` | `worse` |
| `segment-index-get-overlay:getHitAsyncJoin` | `140021.657 ops/s` | `163623.649 ops/s` | `+16.86%` | `better` |
| `segment-index-get-overlay:getHitSync` | `4171276.314 ops/s` | `3936716.745 ops/s` | `-5.62%` | `warning` |
| `segment-index-get-overlay:getMissAsyncJoin` | `163512.203 ops/s` | `158946.402 ops/s` | `-2.79%` | `neutral` |
| `segment-index-get-overlay:getMissSync` | `3765758.725 ops/s` | `3670571.520 ops/s` | `-2.53%` | `neutral` |
| `segment-index-get-persisted:getHitAsyncJoin` | `60911.084 ops/s` | `61893.187 ops/s` | `+1.61%` | `neutral` |
| `segment-index-get-persisted:getHitSync` | `118006.089 ops/s` | `118696.034 ops/s` | `+0.58%` | `neutral` |
| `segment-index-get-persisted:getMissAsyncJoin` | `166584.862 ops/s` | `156265.841 ops/s` | `-6.19%` | `warning` |
| `segment-index-get-persisted:getMissSync` | `4001928.781 ops/s` | `3825206.838 ops/s` | `-4.42%` | `warning` |
| `segment-index-hot-partition-put:putHotPartition` | `2957965.725 ops/s` | `3366835.139 ops/s` | `+13.82%` | `better` |
| `segment-index-hot-partition-put:putThenGetHotPartition` | `1570370.894 ops/s` | `1619821.912 ops/s` | `+3.15%` | `better` |
| `segment-index-lifecycle:openAndCheckAndRepairConsistency` | `242.647 ms/op` | `242.813 ms/op` | `+0.07%` | `neutral` |
| `segment-index-lifecycle:openAndCompact` | `266.607 ms/op` | `266.432 ms/op` | `-0.07%` | `neutral` |
| `segment-index-lifecycle:openExisting` | `240.855 ms/op` | `241.790 ms/op` | `+0.39%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed` | `523136.456 ops/s` | `531987.724 ops/s` | `+1.69%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed:getWorkload` | `517798.614 ops/s` | `526659.263 ops/s` | `+1.71%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed:putWorkload` | `5337.841 ops/s` | `5328.461 ops/s` | `-0.18%` | `neutral` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed` | `261392.275 ops/s` | `271456.521 ops/s` | `+3.85%` | `better` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:getWorkload` | `259950.129 ops/s` | `269907.618 ops/s` | `+3.83%` | `better` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:putWorkload` | `1442.146 ops/s` | `1548.903 ops/s` | `+7.40%` | `better` |
| `segment-index-persisted-mutation:deleteAsyncJoin` | `2075.162 ops/s` | `2090.564 ops/s` | `+0.74%` | `neutral` |
| `segment-index-persisted-mutation:deleteSync` | `2280.231 ops/s` | `2292.814 ops/s` | `+0.55%` | `neutral` |
| `segment-index-persisted-mutation:putAsyncJoin` | `2021.606 ops/s` | `2068.492 ops/s` | `+2.32%` | `neutral` |
| `segment-index-persisted-mutation:putSync` | `2276.163 ops/s` | `2283.469 ops/s` | `+0.32%` | `neutral` |
| `single-chunk-entry-write-compact:writeEntrySteadyState` | `8463010.827 ops/s` | `8330173.938 ops/s` | `-1.57%` | `neutral` |
| `single-chunk-entry-write-large:writeEntrySteadyState` | `8022133.257 ops/s` | `7918703.491 ops/s` | `-1.29%` | `neutral` |
| `sorted-data-diff-key-read-compact:readNextKey` | `9094955.579 ops/s` | `9065127.756 ops/s` | `-0.33%` | `neutral` |
| `sorted-data-diff-key-read-large:readNextKey` | `7508934.029 ops/s` | `7640338.109 ops/s` | `+1.75%` | `neutral` |
