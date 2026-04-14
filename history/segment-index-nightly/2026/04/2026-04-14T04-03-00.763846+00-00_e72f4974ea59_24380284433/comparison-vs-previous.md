# Benchmark Comparison

- Candidate labels rerun 3x total and median-merged because the initial regression exceeded threshold with high relative score error: `segment-index-get-multisegment-cold,segment-index-get-multisegment-hot`


- Profile: `segment-index-nightly`
- Baseline SHA: `e72f4974ea597aa48d2725b131d13e39bcfa6872`
- Candidate SHA: `e72f4974ea597aa48d2725b131d13e39bcfa6872`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `segment-index-get-multisegment-cold:getHitAsyncJoin` | `41.329 ops/s` | `45.807 ops/s` | `+10.83%` | `better` |
| `segment-index-get-multisegment-cold:getHitSync` | `56.471 ops/s` | `43.904 ops/s` | `-22.26%` | `worse` |
| `segment-index-get-multisegment-cold:getMissAsyncJoin` | `169811.025 ops/s` | `168788.256 ops/s` | `-0.60%` | `neutral` |
| `segment-index-get-multisegment-cold:getMissSync` | `3762303.936 ops/s` | `3710966.556 ops/s` | `-1.36%` | `neutral` |
| `segment-index-get-multisegment-hot:getHitAsyncJoin` | `91.590 ops/s` | `94.189 ops/s` | `+2.84%` | `neutral` |
| `segment-index-get-multisegment-hot:getHitSync` | `99.323 ops/s` | `83.801 ops/s` | `-15.63%` | `worse` |
| `segment-index-get-multisegment-hot:getMissAsyncJoin` | `170666.767 ops/s` | `168829.024 ops/s` | `-1.08%` | `neutral` |
| `segment-index-get-multisegment-hot:getMissSync` | `3625872.320 ops/s` | `3528842.937 ops/s` | `-2.68%` | `neutral` |
| `segment-index-get-overlay:getHitAsyncJoin` | `162613.482 ops/s` | `160661.919 ops/s` | `-1.20%` | `neutral` |
| `segment-index-get-overlay:getHitSync` | `3966777.304 ops/s` | `3849709.220 ops/s` | `-2.95%` | `neutral` |
| `segment-index-get-overlay:getMissAsyncJoin` | `154865.730 ops/s` | `159059.457 ops/s` | `+2.71%` | `neutral` |
| `segment-index-get-overlay:getMissSync` | `3969731.610 ops/s` | `4018224.731 ops/s` | `+1.22%` | `neutral` |
| `segment-index-get-persisted:getHitAsyncJoin` | `63647.181 ops/s` | `63304.761 ops/s` | `-0.54%` | `neutral` |
| `segment-index-get-persisted:getHitSync` | `115634.294 ops/s` | `116766.458 ops/s` | `+0.98%` | `neutral` |
| `segment-index-get-persisted:getMissAsyncJoin` | `163590.753 ops/s` | `150199.857 ops/s` | `-8.19%` | `worse` |
| `segment-index-get-persisted:getMissSync` | `4068153.285 ops/s` | `3867135.784 ops/s` | `-4.94%` | `warning` |
| `segment-index-hot-partition-put:putHotPartition` | `3008574.489 ops/s` | `2806559.431 ops/s` | `-6.71%` | `warning` |
| `segment-index-hot-partition-put:putThenGetHotPartition` | `1620752.632 ops/s` | `1608682.771 ops/s` | `-0.74%` | `neutral` |
| `segment-index-lifecycle:openAndCheckAndRepairConsistency` | `247.027 ms/op` | `247.937 ms/op` | `+0.37%` | `neutral` |
| `segment-index-lifecycle:openAndCompact` | `267.149 ms/op` | `269.481 ms/op` | `+0.87%` | `neutral` |
| `segment-index-lifecycle:openExisting` | `246.006 ms/op` | `245.658 ms/op` | `-0.14%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed` | `526659.295 ops/s` | `449806.380 ops/s` | `-14.59%` | `worse` |
| `segment-index-mixed-drain:partitionedIngestMixed:getWorkload` | `521357.919 ops/s` | `444517.160 ops/s` | `-14.74%` | `worse` |
| `segment-index-mixed-drain:partitionedIngestMixed:putWorkload` | `5301.376 ops/s` | `5289.219 ops/s` | `-0.23%` | `neutral` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed` | `257983.101 ops/s` | `275449.536 ops/s` | `+6.77%` | `better` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:getWorkload` | `256611.834 ops/s` | `274124.771 ops/s` | `+6.82%` | `better` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:putWorkload` | `1371.267 ops/s` | `1324.766 ops/s` | `-3.39%` | `warning` |
| `segment-index-persisted-mutation:deleteAsyncJoin` | `2033.800 ops/s` | `1997.727 ops/s` | `-1.77%` | `neutral` |
| `segment-index-persisted-mutation:deleteSync` | `2264.646 ops/s` | `2213.200 ops/s` | `-2.27%` | `neutral` |
| `segment-index-persisted-mutation:putAsyncJoin` | `1940.969 ops/s` | `2007.003 ops/s` | `+3.40%` | `better` |
| `segment-index-persisted-mutation:putSync` | `2176.208 ops/s` | `2162.158 ops/s` | `-0.65%` | `neutral` |
| `single-chunk-entry-write-compact:writeEntrySteadyState` | `8552802.049 ops/s` | `8305629.514 ops/s` | `-2.89%` | `neutral` |
| `single-chunk-entry-write-large:writeEntrySteadyState` | `7939654.697 ops/s` | `7944777.371 ops/s` | `+0.06%` | `neutral` |
| `sorted-data-diff-key-read-compact:readNextKey` | `8640934.183 ops/s` | `8623506.278 ops/s` | `-0.20%` | `neutral` |
| `sorted-data-diff-key-read-large:readNextKey` | `6963478.464 ops/s` | `7485230.804 ops/s` | `+7.49%` | `better` |
