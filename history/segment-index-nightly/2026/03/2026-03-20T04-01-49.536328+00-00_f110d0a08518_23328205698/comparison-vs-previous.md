# Benchmark Comparison

- Profile: `segment-index-nightly`
- Baseline SHA: `d1f9e8964ccadd6dc60e2c4cae6cfc20a09f6e5f`
- Candidate SHA: `f110d0a08518e6d6f918649d4aa14d48cfb15719`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `segment-index-get-multisegment-cold:getHitAsyncJoin` | `42.182 ops/s` | `44.260 ops/s` | `+4.93%` | `better` |
| `segment-index-get-multisegment-cold:getHitSync` | `39.859 ops/s` | `46.801 ops/s` | `+17.42%` | `better` |
| `segment-index-get-multisegment-cold:getMissAsyncJoin` | `166466.758 ops/s` | `165307.039 ops/s` | `-0.70%` | `neutral` |
| `segment-index-get-multisegment-cold:getMissSync` | `3792677.388 ops/s` | `3860497.203 ops/s` | `+1.79%` | `neutral` |
| `segment-index-get-multisegment-hot:getHitAsyncJoin` | `104.397 ops/s` | `93.544 ops/s` | `-10.40%` | `worse` |
| `segment-index-get-multisegment-hot:getHitSync` | `86.474 ops/s` | `84.601 ops/s` | `-2.17%` | `neutral` |
| `segment-index-get-multisegment-hot:getMissAsyncJoin` | `167464.379 ops/s` | `165718.971 ops/s` | `-1.04%` | `neutral` |
| `segment-index-get-multisegment-hot:getMissSync` | `3788840.720 ops/s` | `3551841.902 ops/s` | `-6.26%` | `warning` |
| `segment-index-get-overlay:getHitAsyncJoin` | `155750.150 ops/s` | `164425.109 ops/s` | `+5.57%` | `better` |
| `segment-index-get-overlay:getHitSync` | `3106898.280 ops/s` | `4072546.157 ops/s` | `+31.08%` | `better` |
| `segment-index-get-overlay:getMissAsyncJoin` | `166322.527 ops/s` | `167885.264 ops/s` | `+0.94%` | `neutral` |
| `segment-index-get-overlay:getMissSync` | `3893920.499 ops/s` | `3930683.999 ops/s` | `+0.94%` | `neutral` |
| `segment-index-get-persisted:getHitAsyncJoin` | `59637.868 ops/s` | `60664.106 ops/s` | `+1.72%` | `neutral` |
| `segment-index-get-persisted:getHitSync` | `114583.942 ops/s` | `112304.059 ops/s` | `-1.99%` | `neutral` |
| `segment-index-get-persisted:getMissAsyncJoin` | `167334.205 ops/s` | `160479.321 ops/s` | `-4.10%` | `warning` |
| `segment-index-get-persisted:getMissSync` | `3526668.771 ops/s` | `3759259.011 ops/s` | `+6.60%` | `better` |
| `segment-index-hot-partition-put:putHotPartition` | `2815421.963 ops/s` | `3218721.716 ops/s` | `+14.32%` | `better` |
| `segment-index-hot-partition-put:putThenGetHotPartition` | `1608964.407 ops/s` | `1543976.629 ops/s` | `-4.04%` | `warning` |
| `segment-index-lifecycle:openAndCheckAndRepairConsistency` | `306.263 ms/op` | `303.871 ms/op` | `-0.78%` | `neutral` |
| `segment-index-lifecycle:openAndCompact` | `337.023 ms/op` | `328.397 ms/op` | `-2.56%` | `neutral` |
| `segment-index-lifecycle:openExisting` | `301.859 ms/op` | `301.423 ms/op` | `-0.14%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed` | `505691.622 ops/s` | `483716.851 ops/s` | `-4.35%` | `warning` |
| `segment-index-mixed-drain:partitionedIngestMixed:getWorkload` | `500343.261 ops/s` | `478336.054 ops/s` | `-4.40%` | `warning` |
| `segment-index-mixed-drain:partitionedIngestMixed:putWorkload` | `5348.361 ops/s` | `5380.797 ops/s` | `+0.61%` | `neutral` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed` | `256015.722 ops/s` | `253174.301 ops/s` | `-1.11%` | `neutral` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:getWorkload` | `254396.654 ops/s` | `251531.561 ops/s` | `-1.13%` | `neutral` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:putWorkload` | `1619.068 ops/s` | `1642.739 ops/s` | `+1.46%` | `neutral` |
| `segment-index-persisted-mutation:deleteAsyncJoin` | `2113.365 ops/s` | `2116.532 ops/s` | `+0.15%` | `neutral` |
| `segment-index-persisted-mutation:deleteSync` | `2327.079 ops/s` | `2278.847 ops/s` | `-2.07%` | `neutral` |
| `segment-index-persisted-mutation:putAsyncJoin` | `2047.638 ops/s` | `2041.811 ops/s` | `-0.28%` | `neutral` |
| `segment-index-persisted-mutation:putSync` | `2257.753 ops/s` | `2232.365 ops/s` | `-1.12%` | `neutral` |
