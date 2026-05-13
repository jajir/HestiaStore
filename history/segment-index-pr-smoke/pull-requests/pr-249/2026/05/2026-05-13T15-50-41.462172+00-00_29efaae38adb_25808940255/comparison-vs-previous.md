# Benchmark Comparison

- Profile: `segment-index-pr-smoke`
- Baseline SHA: `e942e650381bf00db7c2fbb3790ab0c49b708f39`
- Candidate SHA: `29efaae38adb4c4821ead8cd0bb7690510be83e4`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `segment-index-get-live:getHitSync` | `2254681.577 ops/s` | `2073000.241 ops/s` | `-8.06%` | `worse` |
| `segment-index-get-live:getMissSync` | `3457913.492 ops/s` | `3548100.751 ops/s` | `+2.61%` | `neutral` |
| `segment-index-get-multisegment-hot:getHitSync` | `7360.378 ops/s` | `7919.622 ops/s` | `+7.60%` | `better` |
| `segment-index-get-multisegment-hot:getMissSync` | `3446357.833 ops/s` | `3287916.543 ops/s` | `-4.60%` | `warning` |
| `segment-index-get-persisted:getHitSync` | `2148986.104 ops/s` | `1877826.941 ops/s` | `-12.62%` | `worse` |
| `segment-index-get-persisted:getMissSync` | `3570155.400 ops/s` | `3559875.906 ops/s` | `-0.29%` | `neutral` |
| `segment-index-hot-route-put:putHotRoute` | `2130268.505 ops/s` | `1993860.798 ops/s` | `-6.40%` | `warning` |
| `segment-index-hot-route-put:putThenGetHotRoute` | `1114168.128 ops/s` | `1067520.780 ops/s` | `-4.19%` | `warning` |
| `segment-index-mixed-drain:partitionedIngestMixed` | `303208.882 ops/s` | `278652.538 ops/s` | `-8.10%` | `worse` |
| `segment-index-mixed-drain:partitionedIngestMixed:getWorkload` | `143640.284 ops/s` | `114494.867 ops/s` | `-20.29%` | `worse` |
| `segment-index-mixed-drain:partitionedIngestMixed:putWorkload` | `159568.599 ops/s` | `164157.671 ops/s` | `+2.88%` | `neutral` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed` | `47644.498 ops/s` | `44648.001 ops/s` | `-6.29%` | `warning` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:getWorkload` | `42404.561 ops/s` | `39367.600 ops/s` | `-7.16%` | `worse` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:putWorkload` | `5239.937 ops/s` | `5280.401 ops/s` | `+0.77%` | `neutral` |
