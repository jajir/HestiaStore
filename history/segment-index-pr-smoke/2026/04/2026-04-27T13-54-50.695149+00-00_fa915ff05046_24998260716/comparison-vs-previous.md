# Benchmark Comparison

- Profile: `segment-index-pr-smoke`
- Baseline SHA: `c51a0e1ce8c5e39b86d5cd734f681c12b84a66c3`
- Candidate SHA: `fa915ff050464f4b44f0424ba5745d7dd4343f06`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `segment-index-get-live:getHitSync` | `2266207.122 ops/s` | `2305079.322 ops/s` | `+1.72%` | `neutral` |
| `segment-index-get-live:getMissSync` | `3639617.764 ops/s` | `3547611.763 ops/s` | `-2.53%` | `neutral` |
| `segment-index-get-multisegment-hot:getHitSync` | `7312.913 ops/s` | `7530.510 ops/s` | `+2.98%` | `neutral` |
| `segment-index-get-multisegment-hot:getMissSync` | `3531603.304 ops/s` | `3562090.809 ops/s` | `+0.86%` | `neutral` |
| `segment-index-get-persisted:getHitSync` | `120310.874 ops/s` | `98445.437 ops/s` | `-18.17%` | `worse` |
| `segment-index-get-persisted:getMissSync` | `3693532.752 ops/s` | `3566111.797 ops/s` | `-3.45%` | `warning` |
| `segment-index-hot-route-put:putHotRoute` | `2061756.751 ops/s` | `1989434.517 ops/s` | `-3.51%` | `warning` |
| `segment-index-hot-route-put:putThenGetHotRoute` | `1080519.198 ops/s` | `1073847.391 ops/s` | `-0.62%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed` | `278916.911 ops/s` | `283616.743 ops/s` | `+1.69%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed:getWorkload` | `128185.949 ops/s` | `117934.530 ops/s` | `-8.00%` | `worse` |
| `segment-index-mixed-drain:partitionedIngestMixed:putWorkload` | `150730.962 ops/s` | `165682.213 ops/s` | `+9.92%` | `better` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed` | `44268.850 ops/s` | `40936.180 ops/s` | `-7.53%` | `worse` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:getWorkload` | `38969.170 ops/s` | `35611.415 ops/s` | `-8.62%` | `worse` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:putWorkload` | `5299.680 ops/s` | `5324.765 ops/s` | `+0.47%` | `neutral` |
| `segment-index-persisted-mutation:deleteSync` | `3541.414 ops/s` | `3748.212 ops/s` | `+5.84%` | `better` |
| `segment-index-persisted-mutation:putSync` | `462.874 ops/s` | `462.161 ops/s` | `-0.15%` | `neutral` |
