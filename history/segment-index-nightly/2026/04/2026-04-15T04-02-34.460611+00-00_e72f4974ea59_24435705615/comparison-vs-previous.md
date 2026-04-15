# Benchmark Comparison

- Profile: `segment-index-nightly`
- Baseline SHA: `e72f4974ea597aa48d2725b131d13e39bcfa6872`
- Candidate SHA: `e72f4974ea597aa48d2725b131d13e39bcfa6872`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `segment-index-get-multisegment-cold:getHitAsyncJoin` | `45.807 ops/s` | `48.998 ops/s` | `+6.97%` | `better` |
| `segment-index-get-multisegment-cold:getHitSync` | `43.904 ops/s` | `47.364 ops/s` | `+7.88%` | `better` |
| `segment-index-get-multisegment-cold:getMissAsyncJoin` | `168788.256 ops/s` | `236959.969 ops/s` | `+40.39%` | `better` |
| `segment-index-get-multisegment-cold:getMissSync` | `3710966.556 ops/s` | `2380892.700 ops/s` | `-35.84%` | `worse` |
| `segment-index-get-multisegment-hot:getHitAsyncJoin` | `94.189 ops/s` | `103.565 ops/s` | `+9.95%` | `better` |
| `segment-index-get-multisegment-hot:getHitSync` | `83.801 ops/s` | `96.472 ops/s` | `+15.12%` | `better` |
| `segment-index-get-multisegment-hot:getMissAsyncJoin` | `168829.024 ops/s` | `235029.990 ops/s` | `+39.21%` | `better` |
| `segment-index-get-multisegment-hot:getMissSync` | `3528842.937 ops/s` | `2220255.463 ops/s` | `-37.08%` | `worse` |
| `segment-index-get-overlay:getHitAsyncJoin` | `160661.919 ops/s` | `230294.214 ops/s` | `+43.34%` | `better` |
| `segment-index-get-overlay:getHitSync` | `3849709.220 ops/s` | `2845167.783 ops/s` | `-26.09%` | `worse` |
| `segment-index-get-overlay:getMissAsyncJoin` | `159059.457 ops/s` | `229681.991 ops/s` | `+44.40%` | `better` |
| `segment-index-get-overlay:getMissSync` | `4018224.731 ops/s` | `2351117.875 ops/s` | `-41.49%` | `worse` |
| `segment-index-get-persisted:getHitAsyncJoin` | `63304.761 ops/s` | `72476.642 ops/s` | `+14.49%` | `better` |
| `segment-index-get-persisted:getHitSync` | `116766.458 ops/s` | `108068.401 ops/s` | `-7.45%` | `worse` |
| `segment-index-get-persisted:getMissAsyncJoin` | `150199.857 ops/s` | `220713.573 ops/s` | `+46.95%` | `better` |
| `segment-index-get-persisted:getMissSync` | `3867135.784 ops/s` | `2468565.884 ops/s` | `-36.17%` | `worse` |
| `segment-index-hot-partition-put:putHotPartition` | `2806559.431 ops/s` | `1989636.437 ops/s` | `-29.11%` | `worse` |
| `segment-index-hot-partition-put:putThenGetHotPartition` | `1608682.771 ops/s` | `1009209.819 ops/s` | `-37.26%` | `worse` |
| `segment-index-lifecycle:openAndCheckAndRepairConsistency` | `247.937 ms/op` | `132.644 ms/op` | `-46.50%` | `worse` |
| `segment-index-lifecycle:openAndCompact` | `269.481 ms/op` | `155.719 ms/op` | `-42.22%` | `worse` |
| `segment-index-lifecycle:openExisting` | `245.658 ms/op` | `132.443 ms/op` | `-46.09%` | `worse` |
| `segment-index-mixed-drain:partitionedIngestMixed` | `449806.380 ops/s` | `466434.660 ops/s` | `+3.70%` | `better` |
| `segment-index-mixed-drain:partitionedIngestMixed:getWorkload` | `444517.160 ops/s` | `461100.972 ops/s` | `+3.73%` | `better` |
| `segment-index-mixed-drain:partitionedIngestMixed:putWorkload` | `5289.219 ops/s` | `5333.689 ops/s` | `+0.84%` | `neutral` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed` | `275449.536 ops/s` | `247326.274 ops/s` | `-10.21%` | `worse` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:getWorkload` | `274124.771 ops/s` | `245908.989 ops/s` | `-10.29%` | `worse` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:putWorkload` | `1324.766 ops/s` | `1417.285 ops/s` | `+6.98%` | `better` |
| `segment-index-persisted-mutation:deleteAsyncJoin` | `1997.727 ops/s` | `2516.316 ops/s` | `+25.96%` | `better` |
| `segment-index-persisted-mutation:deleteSync` | `2213.200 ops/s` | `2794.119 ops/s` | `+26.25%` | `better` |
| `segment-index-persisted-mutation:putAsyncJoin` | `2007.003 ops/s` | `2445.476 ops/s` | `+21.85%` | `better` |
| `segment-index-persisted-mutation:putSync` | `2162.158 ops/s` | `2734.499 ops/s` | `+26.47%` | `better` |
| `single-chunk-entry-write-compact:writeEntrySteadyState` | `8305629.514 ops/s` | `8899094.954 ops/s` | `+7.15%` | `better` |
| `single-chunk-entry-write-large:writeEntrySteadyState` | `7944777.371 ops/s` | `8057064.537 ops/s` | `+1.41%` | `neutral` |
| `sorted-data-diff-key-read-compact:readNextKey` | `8623506.278 ops/s` | `8167049.148 ops/s` | `-5.29%` | `warning` |
| `sorted-data-diff-key-read-large:readNextKey` | `7485230.804 ops/s` | `7052356.101 ops/s` | `-5.78%` | `warning` |
