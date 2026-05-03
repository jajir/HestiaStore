# Benchmark Comparison

- Candidate labels rerun 3x total and median-merged because the initial regression exceeded threshold with high relative score error: `segment-index-get-multisegment-hot`


- Profile: `segment-index-nightly`
- Baseline SHA: `e72f4974ea597aa48d2725b131d13e39bcfa6872`
- Candidate SHA: `e72f4974ea597aa48d2725b131d13e39bcfa6872`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `segment-index-get-multisegment-cold:getHitAsyncJoin` | `48.884 ops/s` | `47.317 ops/s` | `-3.20%` | `warning` |
| `segment-index-get-multisegment-cold:getHitSync` | `35.803 ops/s` | `51.466 ops/s` | `+43.75%` | `better` |
| `segment-index-get-multisegment-cold:getMissAsyncJoin` | `171641.098 ops/s` | `170140.555 ops/s` | `-0.87%` | `neutral` |
| `segment-index-get-multisegment-cold:getMissSync` | `3751084.751 ops/s` | `3794019.379 ops/s` | `+1.14%` | `neutral` |
| `segment-index-get-multisegment-hot:getHitAsyncJoin` | `94.212 ops/s` | `106.686 ops/s` | `+13.24%` | `better` |
| `segment-index-get-multisegment-hot:getHitSync` | `92.337 ops/s` | `85.362 ops/s` | `-7.55%` | `worse` |
| `segment-index-get-multisegment-hot:getMissAsyncJoin` | `173048.267 ops/s` | `173351.102 ops/s` | `+0.18%` | `neutral` |
| `segment-index-get-multisegment-hot:getMissSync` | `4039407.259 ops/s` | `4041814.586 ops/s` | `+0.06%` | `neutral` |
| `segment-index-get-overlay:getHitAsyncJoin` | `159257.799 ops/s` | `164414.099 ops/s` | `+3.24%` | `better` |
| `segment-index-get-overlay:getHitSync` | `4067505.767 ops/s` | `4210391.618 ops/s` | `+3.51%` | `better` |
| `segment-index-get-overlay:getMissAsyncJoin` | `166623.788 ops/s` | `159778.140 ops/s` | `-4.11%` | `warning` |
| `segment-index-get-overlay:getMissSync` | `4231170.250 ops/s` | `4082425.438 ops/s` | `-3.52%` | `warning` |
| `segment-index-get-persisted:getHitAsyncJoin` | `63679.739 ops/s` | `66416.791 ops/s` | `+4.30%` | `better` |
| `segment-index-get-persisted:getHitSync` | `114880.721 ops/s` | `116861.916 ops/s` | `+1.72%` | `neutral` |
| `segment-index-get-persisted:getMissAsyncJoin` | `163707.160 ops/s` | `167285.987 ops/s` | `+2.19%` | `neutral` |
| `segment-index-get-persisted:getMissSync` | `3548040.019 ops/s` | `4059657.674 ops/s` | `+14.42%` | `better` |
| `segment-index-hot-partition-put:putHotPartition` | `3167180.957 ops/s` | `3046865.981 ops/s` | `-3.80%` | `warning` |
| `segment-index-hot-partition-put:putThenGetHotPartition` | `1532464.878 ops/s` | `1437556.165 ops/s` | `-6.19%` | `warning` |
| `segment-index-lifecycle:openAndCheckAndRepairConsistency` | `243.768 ms/op` | `241.292 ms/op` | `-1.02%` | `neutral` |
| `segment-index-lifecycle:openAndCompact` | `265.710 ms/op` | `262.720 ms/op` | `-1.13%` | `neutral` |
| `segment-index-lifecycle:openExisting` | `238.052 ms/op` | `234.338 ms/op` | `-1.56%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed` | `523263.851 ops/s` | `535921.061 ops/s` | `+2.42%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed:getWorkload` | `517922.526 ops/s` | `530618.734 ops/s` | `+2.45%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed:putWorkload` | `5341.324 ops/s` | `5302.327 ops/s` | `-0.73%` | `neutral` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed` | `264643.785 ops/s` | `-` | `-` | `removed` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:getWorkload` | `263098.124 ops/s` | `-` | `-` | `removed` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:putWorkload` | `1545.661 ops/s` | `-` | `-` | `removed` |
| `segment-index-persisted-mutation:deleteAsyncJoin` | `2046.638 ops/s` | `2088.415 ops/s` | `+2.04%` | `neutral` |
| `segment-index-persisted-mutation:deleteSync` | `2256.077 ops/s` | `2289.371 ops/s` | `+1.48%` | `neutral` |
| `segment-index-persisted-mutation:putAsyncJoin` | `2028.445 ops/s` | `2091.631 ops/s` | `+3.12%` | `better` |
| `segment-index-persisted-mutation:putSync` | `2108.759 ops/s` | `2261.788 ops/s` | `+7.26%` | `better` |
| `single-chunk-entry-write-compact:writeEntrySteadyState` | `8565823.167 ops/s` | `6551513.787 ops/s` | `-23.52%` | `worse` |
| `single-chunk-entry-write-large:writeEntrySteadyState` | `7665129.599 ops/s` | `8015812.660 ops/s` | `+4.58%` | `better` |
| `sorted-data-diff-key-read-compact:readNextKey` | `9017111.023 ops/s` | `8676242.070 ops/s` | `-3.78%` | `warning` |
| `sorted-data-diff-key-read-large:readNextKey` | `7536770.502 ops/s` | `7393406.929 ops/s` | `-1.90%` | `neutral` |
