# Benchmark Comparison

- Profile: `segment-index-nightly`
- Baseline SHA: `e72f4974ea597aa48d2725b131d13e39bcfa6872`
- Candidate SHA: `e72f4974ea597aa48d2725b131d13e39bcfa6872`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `segment-index-get-multisegment-cold:getHitAsyncJoin` | `44.858 ops/s` | `44.588 ops/s` | `-0.60%` | `neutral` |
| `segment-index-get-multisegment-cold:getHitSync` | `46.454 ops/s` | `53.367 ops/s` | `+14.88%` | `better` |
| `segment-index-get-multisegment-cold:getMissAsyncJoin` | `170995.247 ops/s` | `172199.675 ops/s` | `+0.70%` | `neutral` |
| `segment-index-get-multisegment-cold:getMissSync` | `3602333.240 ops/s` | `4001960.958 ops/s` | `+11.09%` | `better` |
| `segment-index-get-multisegment-hot:getHitAsyncJoin` | `90.899 ops/s` | `94.963 ops/s` | `+4.47%` | `better` |
| `segment-index-get-multisegment-hot:getHitSync` | `89.008 ops/s` | `82.793 ops/s` | `-6.98%` | `warning` |
| `segment-index-get-multisegment-hot:getMissAsyncJoin` | `172106.983 ops/s` | `171754.618 ops/s` | `-0.20%` | `neutral` |
| `segment-index-get-multisegment-hot:getMissSync` | `3658338.670 ops/s` | `3469153.716 ops/s` | `-5.17%` | `warning` |
| `segment-index-get-overlay:getHitAsyncJoin` | `154233.267 ops/s` | `165085.195 ops/s` | `+7.04%` | `better` |
| `segment-index-get-overlay:getHitSync` | `4112327.116 ops/s` | `3767196.717 ops/s` | `-8.39%` | `worse` |
| `segment-index-get-overlay:getMissAsyncJoin` | `166342.814 ops/s` | `164325.883 ops/s` | `-1.21%` | `neutral` |
| `segment-index-get-overlay:getMissSync` | `3699481.502 ops/s` | `3552788.672 ops/s` | `-3.97%` | `warning` |
| `segment-index-get-persisted:getHitAsyncJoin` | `63619.991 ops/s` | `60574.799 ops/s` | `-4.79%` | `warning` |
| `segment-index-get-persisted:getHitSync` | `115793.282 ops/s` | `113327.836 ops/s` | `-2.13%` | `neutral` |
| `segment-index-get-persisted:getMissAsyncJoin` | `160839.781 ops/s` | `164442.723 ops/s` | `+2.24%` | `neutral` |
| `segment-index-get-persisted:getMissSync` | `3827199.676 ops/s` | `4152657.488 ops/s` | `+8.50%` | `better` |
| `segment-index-hot-partition-put:putHotPartition` | `3018352.345 ops/s` | `3047695.327 ops/s` | `+0.97%` | `neutral` |
| `segment-index-hot-partition-put:putThenGetHotPartition` | `1630754.796 ops/s` | `1555331.610 ops/s` | `-4.63%` | `warning` |
| `segment-index-lifecycle:openAndCheckAndRepairConsistency` | `246.202 ms/op` | `242.150 ms/op` | `-1.65%` | `neutral` |
| `segment-index-lifecycle:openAndCompact` | `268.270 ms/op` | `266.261 ms/op` | `-0.75%` | `neutral` |
| `segment-index-lifecycle:openExisting` | `243.149 ms/op` | `241.193 ms/op` | `-0.80%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed` | `521470.364 ops/s` | `516968.533 ops/s` | `-0.86%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed:getWorkload` | `516165.955 ops/s` | `511661.230 ops/s` | `-0.87%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed:putWorkload` | `5304.410 ops/s` | `5307.303 ops/s` | `+0.05%` | `neutral` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed` | `271278.749 ops/s` | `266157.192 ops/s` | `-1.89%` | `neutral` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:getWorkload` | `264211.272 ops/s` | `264706.839 ops/s` | `+0.19%` | `neutral` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:putWorkload` | `1520.983 ops/s` | `1450.353 ops/s` | `-4.64%` | `warning` |
| `segment-index-persisted-mutation:deleteAsyncJoin` | `2100.731 ops/s` | `1965.911 ops/s` | `-6.42%` | `warning` |
| `segment-index-persisted-mutation:deleteSync` | `2287.295 ops/s` | `2126.259 ops/s` | `-7.04%` | `worse` |
| `segment-index-persisted-mutation:putAsyncJoin` | `2034.758 ops/s` | `1923.707 ops/s` | `-5.46%` | `warning` |
| `segment-index-persisted-mutation:putSync` | `2217.413 ops/s` | `2115.026 ops/s` | `-4.62%` | `warning` |
| `single-chunk-entry-write-compact:writeEntrySteadyState` | `8461280.408 ops/s` | `8379548.778 ops/s` | `-0.97%` | `neutral` |
| `single-chunk-entry-write-large:writeEntrySteadyState` | `7498331.852 ops/s` | `7940359.689 ops/s` | `+5.90%` | `better` |
| `sorted-data-diff-key-read-compact:readNextKey` | `8690269.476 ops/s` | `9479790.837 ops/s` | `+9.09%` | `better` |
| `sorted-data-diff-key-read-large:readNextKey` | `7069965.756 ops/s` | `7496036.998 ops/s` | `+6.03%` | `better` |
