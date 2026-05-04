# Benchmark Comparison

- Candidate labels rerun 3x total and median-merged because the initial regression exceeded threshold with high relative score error: `segment-index-get-multisegment-cold,segment-index-get-multisegment-hot,segment-index-get-overlay,segment-index-get-persisted`


- Profile: `segment-index-nightly`
- Baseline SHA: `a87e6e844c0f83bfc93f8ff3748153d6ffec3dbb`
- Candidate SHA: `e72f4974ea597aa48d2725b131d13e39bcfa6872`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `segment-index-get-multisegment-cold:getHitAsyncJoin` | `50.747 ops/s` | `46.290 ops/s` | `-8.78%` | `worse` |
| `segment-index-get-multisegment-cold:getHitSync` | `52.964 ops/s` | `48.334 ops/s` | `-8.74%` | `worse` |
| `segment-index-get-multisegment-cold:getMissAsyncJoin` | `173079.411 ops/s` | `171715.756 ops/s` | `-0.79%` | `neutral` |
| `segment-index-get-multisegment-cold:getMissSync` | `3471348.762 ops/s` | `3831807.317 ops/s` | `+10.38%` | `better` |
| `segment-index-get-multisegment-hot:getHitAsyncJoin` | `92.367 ops/s` | `102.618 ops/s` | `+11.10%` | `better` |
| `segment-index-get-multisegment-hot:getHitSync` | `92.367 ops/s` | `84.399 ops/s` | `-8.63%` | `worse` |
| `segment-index-get-multisegment-hot:getMissAsyncJoin` | `174007.589 ops/s` | `172312.608 ops/s` | `-0.97%` | `neutral` |
| `segment-index-get-multisegment-hot:getMissSync` | `4159698.890 ops/s` | `3777903.414 ops/s` | `-9.18%` | `worse` |
| `segment-index-get-overlay:getHitAsyncJoin` | `162762.197 ops/s` | `162744.509 ops/s` | `-0.01%` | `neutral` |
| `segment-index-get-overlay:getHitSync` | `4225640.986 ops/s` | `3699562.911 ops/s` | `-12.45%` | `worse` |
| `segment-index-get-overlay:getMissAsyncJoin` | `159214.046 ops/s` | `165205.508 ops/s` | `+3.76%` | `better` |
| `segment-index-get-overlay:getMissSync` | `3693889.363 ops/s` | `3844601.370 ops/s` | `+4.08%` | `better` |
| `segment-index-get-persisted:getHitAsyncJoin` | `62419.799 ops/s` | `65240.515 ops/s` | `+4.52%` | `better` |
| `segment-index-get-persisted:getHitSync` | `117365.104 ops/s` | `116764.210 ops/s` | `-0.51%` | `neutral` |
| `segment-index-get-persisted:getMissAsyncJoin` | `166676.195 ops/s` | `165328.115 ops/s` | `-0.81%` | `neutral` |
| `segment-index-get-persisted:getMissSync` | `4194872.261 ops/s` | `3573890.580 ops/s` | `-14.80%` | `worse` |
| `segment-index-hot-partition-put:putHotPartition` | `2906505.488 ops/s` | `2959976.022 ops/s` | `+1.84%` | `neutral` |
| `segment-index-hot-partition-put:putThenGetHotPartition` | `1645733.679 ops/s` | `1727755.127 ops/s` | `+4.98%` | `better` |
| `segment-index-lifecycle:openAndCheckAndRepairConsistency` | `241.786 ms/op` | `243.624 ms/op` | `+0.76%` | `neutral` |
| `segment-index-lifecycle:openAndCompact` | `266.585 ms/op` | `264.901 ms/op` | `-0.63%` | `neutral` |
| `segment-index-lifecycle:openExisting` | `238.743 ms/op` | `240.183 ms/op` | `+0.60%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed` | `521211.184 ops/s` | `529038.216 ops/s` | `+1.50%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed:getWorkload` | `515878.399 ops/s` | `523731.134 ops/s` | `+1.52%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed:putWorkload` | `5332.785 ops/s` | `5307.081 ops/s` | `-0.48%` | `neutral` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed` | `263021.668 ops/s` | `266137.643 ops/s` | `+1.18%` | `neutral` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:getWorkload` | `261530.653 ops/s` | `264710.601 ops/s` | `+1.22%` | `neutral` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:putWorkload` | `1491.015 ops/s` | `1427.042 ops/s` | `-4.29%` | `warning` |
| `segment-index-persisted-mutation:deleteAsyncJoin` | `2004.456 ops/s` | `1997.562 ops/s` | `-0.34%` | `neutral` |
| `segment-index-persisted-mutation:deleteSync` | `2233.187 ops/s` | `2221.430 ops/s` | `-0.53%` | `neutral` |
| `segment-index-persisted-mutation:putAsyncJoin` | `1994.934 ops/s` | `1979.378 ops/s` | `-0.78%` | `neutral` |
| `segment-index-persisted-mutation:putSync` | `2175.188 ops/s` | `2166.124 ops/s` | `-0.42%` | `neutral` |
| `single-chunk-entry-write-compact:writeEntrySteadyState` | `8445486.686 ops/s` | `8480804.073 ops/s` | `+0.42%` | `neutral` |
| `single-chunk-entry-write-large:writeEntrySteadyState` | `7892549.505 ops/s` | `7571023.331 ops/s` | `-4.07%` | `warning` |
| `sorted-data-diff-key-read-compact:readNextKey` | `9334883.209 ops/s` | `9459641.674 ops/s` | `+1.34%` | `neutral` |
| `sorted-data-diff-key-read-large:readNextKey` | `7235902.056 ops/s` | `7328077.334 ops/s` | `+1.27%` | `neutral` |
