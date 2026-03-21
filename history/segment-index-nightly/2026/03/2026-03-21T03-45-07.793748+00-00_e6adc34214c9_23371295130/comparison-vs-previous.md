# Benchmark Comparison

- Profile: `segment-index-nightly`
- Baseline SHA: `f110d0a08518e6d6f918649d4aa14d48cfb15719`
- Candidate SHA: `e6adc34214c9ff0a3d8eae146bfacb8b07282732`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `segment-index-get-multisegment-cold:getHitAsyncJoin` | `44.260 ops/s` | `36.394 ops/s` | `-17.77%` | `worse` |
| `segment-index-get-multisegment-cold:getHitSync` | `46.801 ops/s` | `39.983 ops/s` | `-14.57%` | `worse` |
| `segment-index-get-multisegment-cold:getMissAsyncJoin` | `165307.039 ops/s` | `174444.859 ops/s` | `+5.53%` | `better` |
| `segment-index-get-multisegment-cold:getMissSync` | `3860497.203 ops/s` | `3459501.892 ops/s` | `-10.39%` | `worse` |
| `segment-index-get-multisegment-hot:getHitAsyncJoin` | `93.544 ops/s` | `99.120 ops/s` | `+5.96%` | `better` |
| `segment-index-get-multisegment-hot:getHitSync` | `84.601 ops/s` | `101.306 ops/s` | `+19.75%` | `better` |
| `segment-index-get-multisegment-hot:getMissAsyncJoin` | `165718.971 ops/s` | `174956.440 ops/s` | `+5.57%` | `better` |
| `segment-index-get-multisegment-hot:getMissSync` | `3551841.902 ops/s` | `3647410.147 ops/s` | `+2.69%` | `neutral` |
| `segment-index-get-overlay:getHitAsyncJoin` | `164425.109 ops/s` | `170314.575 ops/s` | `+3.58%` | `better` |
| `segment-index-get-overlay:getHitSync` | `4072546.157 ops/s` | `3917583.352 ops/s` | `-3.81%` | `warning` |
| `segment-index-get-overlay:getMissAsyncJoin` | `167885.264 ops/s` | `170064.697 ops/s` | `+1.30%` | `neutral` |
| `segment-index-get-overlay:getMissSync` | `3930683.999 ops/s` | `2972302.466 ops/s` | `-24.38%` | `worse` |
| `segment-index-get-persisted:getHitAsyncJoin` | `60664.106 ops/s` | `59550.351 ops/s` | `-1.84%` | `neutral` |
| `segment-index-get-persisted:getHitSync` | `112304.059 ops/s` | `113588.503 ops/s` | `+1.14%` | `neutral` |
| `segment-index-get-persisted:getMissAsyncJoin` | `160479.321 ops/s` | `173791.160 ops/s` | `+8.30%` | `better` |
| `segment-index-get-persisted:getMissSync` | `3759259.011 ops/s` | `3674595.197 ops/s` | `-2.25%` | `neutral` |
| `segment-index-hot-partition-put:putHotPartition` | `3218721.716 ops/s` | `2687614.649 ops/s` | `-16.50%` | `worse` |
| `segment-index-hot-partition-put:putThenGetHotPartition` | `1543976.629 ops/s` | `1485355.453 ops/s` | `-3.80%` | `warning` |
| `segment-index-lifecycle:openAndCheckAndRepairConsistency` | `303.871 ms/op` | `312.976 ms/op` | `+3.00%` | `neutral` |
| `segment-index-lifecycle:openAndCompact` | `328.397 ms/op` | `333.729 ms/op` | `+1.62%` | `neutral` |
| `segment-index-lifecycle:openExisting` | `301.423 ms/op` | `305.646 ms/op` | `+1.40%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed` | `483716.851 ops/s` | `500653.792 ops/s` | `+3.50%` | `better` |
| `segment-index-mixed-drain:partitionedIngestMixed:getWorkload` | `478336.054 ops/s` | `495302.437 ops/s` | `+3.55%` | `better` |
| `segment-index-mixed-drain:partitionedIngestMixed:putWorkload` | `5380.797 ops/s` | `5351.355 ops/s` | `-0.55%` | `neutral` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed` | `253174.301 ops/s` | `253990.659 ops/s` | `+0.32%` | `neutral` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:getWorkload` | `251531.561 ops/s` | `252488.865 ops/s` | `+0.38%` | `neutral` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:putWorkload` | `1642.739 ops/s` | `1501.794 ops/s` | `-8.58%` | `worse` |
| `segment-index-persisted-mutation:deleteAsyncJoin` | `2116.532 ops/s` | `1895.878 ops/s` | `-10.43%` | `worse` |
| `segment-index-persisted-mutation:deleteSync` | `2278.847 ops/s` | `2067.711 ops/s` | `-9.27%` | `worse` |
| `segment-index-persisted-mutation:putAsyncJoin` | `2041.811 ops/s` | `1793.829 ops/s` | `-12.15%` | `worse` |
| `segment-index-persisted-mutation:putSync` | `2232.365 ops/s` | `2054.687 ops/s` | `-7.96%` | `worse` |
