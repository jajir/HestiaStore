# Benchmark Comparison

- Candidate labels rerun 3x total and median-merged because the initial regression exceeded threshold with high relative score error: `segment-index-get-multisegment-cold,segment-index-lifecycle`


- Profile: `segment-index-nightly`
- Baseline SHA: `e72f4974ea597aa48d2725b131d13e39bcfa6872`
- Candidate SHA: `e72f4974ea597aa48d2725b131d13e39bcfa6872`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `segment-index-get-multisegment-cold:getHitAsyncJoin` | `42.903 ops/s` | `39.748 ops/s` | `-7.35%` | `worse` |
| `segment-index-get-multisegment-cold:getHitSync` | `42.652 ops/s` | `41.604 ops/s` | `-2.46%` | `neutral` |
| `segment-index-get-multisegment-cold:getMissAsyncJoin` | `172845.068 ops/s` | `240081.241 ops/s` | `+38.90%` | `better` |
| `segment-index-get-multisegment-cold:getMissSync` | `3572506.758 ops/s` | `2583482.488 ops/s` | `-27.68%` | `worse` |
| `segment-index-get-multisegment-hot:getHitAsyncJoin` | `100.837 ops/s` | `104.302 ops/s` | `+3.44%` | `better` |
| `segment-index-get-multisegment-hot:getHitSync` | `94.840 ops/s` | `102.118 ops/s` | `+7.67%` | `better` |
| `segment-index-get-multisegment-hot:getMissAsyncJoin` | `171831.339 ops/s` | `240931.788 ops/s` | `+40.21%` | `better` |
| `segment-index-get-multisegment-hot:getMissSync` | `3671588.894 ops/s` | `2464056.760 ops/s` | `-32.89%` | `worse` |
| `segment-index-get-overlay:getHitAsyncJoin` | `164026.828 ops/s` | `232664.479 ops/s` | `+41.85%` | `better` |
| `segment-index-get-overlay:getHitSync` | `3882969.258 ops/s` | `2512338.518 ops/s` | `-35.30%` | `worse` |
| `segment-index-get-overlay:getMissAsyncJoin` | `159826.707 ops/s` | `235653.807 ops/s` | `+47.44%` | `better` |
| `segment-index-get-overlay:getMissSync` | `4056121.306 ops/s` | `2551024.102 ops/s` | `-37.11%` | `worse` |
| `segment-index-get-persisted:getHitAsyncJoin` | `58696.861 ops/s` | `78010.424 ops/s` | `+32.90%` | `better` |
| `segment-index-get-persisted:getHitSync` | `114910.740 ops/s` | `112657.392 ops/s` | `-1.96%` | `neutral` |
| `segment-index-get-persisted:getMissAsyncJoin` | `168937.442 ops/s` | `228443.905 ops/s` | `+35.22%` | `better` |
| `segment-index-get-persisted:getMissSync` | `3879551.368 ops/s` | `2536185.395 ops/s` | `-34.63%` | `worse` |
| `segment-index-hot-partition-put:putHotPartition` | `2846695.185 ops/s` | `1909693.932 ops/s` | `-32.92%` | `worse` |
| `segment-index-hot-partition-put:putThenGetHotPartition` | `1525356.259 ops/s` | `1009142.163 ops/s` | `-33.84%` | `worse` |
| `segment-index-lifecycle:openAndCheckAndRepairConsistency` | `253.900 ms/op` | `133.979 ms/op` | `-47.23%` | `worse` |
| `segment-index-lifecycle:openAndCompact` | `270.969 ms/op` | `155.952 ms/op` | `-42.45%` | `worse` |
| `segment-index-lifecycle:openExisting` | `246.288 ms/op` | `131.863 ms/op` | `-46.46%` | `worse` |
| `segment-index-mixed-drain:partitionedIngestMixed` | `518811.323 ops/s` | `459349.424 ops/s` | `-11.46%` | `worse` |
| `segment-index-mixed-drain:partitionedIngestMixed:getWorkload` | `513518.529 ops/s` | `453980.614 ops/s` | `-11.59%` | `worse` |
| `segment-index-mixed-drain:partitionedIngestMixed:putWorkload` | `5292.794 ops/s` | `5368.810 ops/s` | `+1.44%` | `neutral` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed` | `260908.579 ops/s` | `243565.342 ops/s` | `-6.65%` | `warning` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:getWorkload` | `259281.456 ops/s` | `242089.367 ops/s` | `-6.63%` | `warning` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:putWorkload` | `1627.123 ops/s` | `1475.974 ops/s` | `-9.29%` | `worse` |
| `segment-index-persisted-mutation:deleteAsyncJoin` | `2021.077 ops/s` | `2485.156 ops/s` | `+22.96%` | `better` |
| `segment-index-persisted-mutation:deleteSync` | `2262.882 ops/s` | `2754.755 ops/s` | `+21.74%` | `better` |
| `segment-index-persisted-mutation:putAsyncJoin` | `1995.631 ops/s` | `2505.136 ops/s` | `+25.53%` | `better` |
| `segment-index-persisted-mutation:putSync` | `2208.554 ops/s` | `2690.224 ops/s` | `+21.81%` | `better` |
| `single-chunk-entry-write-compact:writeEntrySteadyState` | `8506146.537 ops/s` | `8893172.402 ops/s` | `+4.55%` | `better` |
| `single-chunk-entry-write-large:writeEntrySteadyState` | `7919612.395 ops/s` | `8051805.162 ops/s` | `+1.67%` | `neutral` |
| `sorted-data-diff-key-read-compact:readNextKey` | `8627218.634 ops/s` | `8156597.829 ops/s` | `-5.46%` | `warning` |
| `sorted-data-diff-key-read-large:readNextKey` | `7048574.773 ops/s` | `7029765.264 ops/s` | `-0.27%` | `neutral` |
