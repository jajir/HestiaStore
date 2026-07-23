# Benchmark Comparison

- Profile: `segment-index-nightly`
- Baseline SHA: `a63e8857313f97e6163f9e4567b7002fa0a469ea`
- Candidate SHA: `96593cd49b374380016362f1fc4d5bacdf709832`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `segment-index-get-live:getHitSync` | `1888703.388 ops/s` | `1974400.796 ops/s` | `+4.54%` | `better` |
| `segment-index-get-live:getMissSync` | `2012619.829 ops/s` | `1925294.526 ops/s` | `-4.34%` | `warning` |
| `segment-index-get-persisted:getHitSync` | `1849463.612 ops/s` | `1451986.731 ops/s` | `-21.49%` | `worse` |
| `segment-index-get-persisted:getMissSync` | `2036117.881 ops/s` | `1868232.184 ops/s` | `-8.25%` | `worse` |
| `segment-index-hot-route-put:putHotRoute` | `2142472.712 ops/s` | `2119679.204 ops/s` | `-1.06%` | `neutral` |
| `segment-index-hot-route-put:putThenGetHotRoute` | `989871.366 ops/s` | `1082788.989 ops/s` | `+9.39%` | `better` |
| `segment-index-lifecycle:openAndCheckAndRepairConsistency` | `308.049 ms/op` | `310.386 ms/op` | `+0.76%` | `neutral` |
| `segment-index-lifecycle:openAndCompact` | `333.646 ms/op` | `329.788 ms/op` | `-1.16%` | `neutral` |
| `segment-index-lifecycle:openExisting` | `306.040 ms/op` | `305.069 ms/op` | `-0.32%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed` | `445945.807 ops/s` | `440214.722 ops/s` | `-1.29%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed:getWorkload` | `201563.922 ops/s` | `194554.404 ops/s` | `-3.48%` | `warning` |
| `segment-index-mixed-drain:partitionedIngestMixed:putWorkload` | `244381.885 ops/s` | `245660.318 ops/s` | `+0.52%` | `neutral` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed` | `902956.254 ops/s` | `881424.267 ops/s` | `-2.38%` | `neutral` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:getWorkload` | `885363.512 ops/s` | `864035.856 ops/s` | `-2.41%` | `neutral` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:putWorkload` | `17592.743 ops/s` | `17388.410 ops/s` | `-1.16%` | `neutral` |
