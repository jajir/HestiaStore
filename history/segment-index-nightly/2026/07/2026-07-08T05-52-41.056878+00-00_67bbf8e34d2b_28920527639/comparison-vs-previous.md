# Benchmark Comparison

- Profile: `segment-index-nightly`
- Baseline SHA: `e898a5940d69a8f46e99d82c0b4f48ffe78308c0`
- Candidate SHA: `67bbf8e34d2b602923270722df932c97760ccab3`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `segment-index-get-live:getHitSync` | `2147415.891 ops/s` | `2321669.520 ops/s` | `+8.11%` | `better` |
| `segment-index-get-live:getMissSync` | `2166990.151 ops/s` | `1995606.813 ops/s` | `-7.91%` | `worse` |
| `segment-index-get-persisted:getHitSync` | `1605213.592 ops/s` | `1459759.015 ops/s` | `-9.06%` | `worse` |
| `segment-index-get-persisted:getMissSync` | `2035603.400 ops/s` | `1998023.668 ops/s` | `-1.85%` | `neutral` |
| `segment-index-hot-route-put:putHotRoute` | `2155124.373 ops/s` | `2060214.591 ops/s` | `-4.40%` | `warning` |
| `segment-index-hot-route-put:putThenGetHotRoute` | `1096907.067 ops/s` | `1134482.116 ops/s` | `+3.43%` | `better` |
| `segment-index-lifecycle:openAndCheckAndRepairConsistency` | `258.480 ms/op` | `253.576 ms/op` | `-1.90%` | `neutral` |
| `segment-index-lifecycle:openAndCompact` | `275.605 ms/op` | `276.902 ms/op` | `+0.47%` | `neutral` |
| `segment-index-lifecycle:openExisting` | `251.912 ms/op` | `251.707 ms/op` | `-0.08%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed` | `442337.124 ops/s` | `438333.602 ops/s` | `-0.91%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed:getWorkload` | `202177.221 ops/s` | `195365.607 ops/s` | `-3.37%` | `warning` |
| `segment-index-mixed-drain:partitionedIngestMixed:putWorkload` | `240159.903 ops/s` | `242967.995 ops/s` | `+1.17%` | `neutral` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed` | `897559.030 ops/s` | `872815.433 ops/s` | `-2.76%` | `neutral` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:getWorkload` | `880647.028 ops/s` | `855799.243 ops/s` | `-2.82%` | `neutral` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:putWorkload` | `16912.003 ops/s` | `17016.190 ops/s` | `+0.62%` | `neutral` |
