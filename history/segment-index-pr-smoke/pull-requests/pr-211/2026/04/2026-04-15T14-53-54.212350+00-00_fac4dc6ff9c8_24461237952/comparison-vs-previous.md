# Benchmark Comparison

- Profile: `segment-index-pr-smoke`
- Baseline SHA: `30f93a9be86df7361bbeb5cf93c3547cbb155d00`
- Candidate SHA: `fac4dc6ff9c8ac9bbdf506650abdc72e6697ba7f`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `segment-index-get-live:getHitSync` | `2643017.139 ops/s` | `2694565.794 ops/s` | `+1.95%` | `neutral` |
| `segment-index-get-live:getMissSync` | `2597384.493 ops/s` | `2528096.525 ops/s` | `-2.67%` | `neutral` |
| `segment-index-get-multisegment-hot:getHitSync` | `84.200 ops/s` | `92.826 ops/s` | `+10.24%` | `better` |
| `segment-index-get-multisegment-hot:getMissSync` | `2592561.310 ops/s` | `2579730.963 ops/s` | `-0.49%` | `neutral` |
| `segment-index-get-persisted:getHitSync` | `114782.135 ops/s` | `118281.847 ops/s` | `+3.05%` | `better` |
| `segment-index-get-persisted:getMissSync` | `2440712.428 ops/s` | `2551811.408 ops/s` | `+4.55%` | `better` |
| `segment-index-hot-route-put:putHotRoute` | `2242243.097 ops/s` | `2334518.694 ops/s` | `+4.12%` | `better` |
| `segment-index-hot-route-put:putThenGetHotRoute` | `1166294.288 ops/s` | `1158731.017 ops/s` | `-0.65%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed` | `293987.650 ops/s` | `311717.296 ops/s` | `+6.03%` | `better` |
| `segment-index-mixed-drain:partitionedIngestMixed:getWorkload` | `148571.856 ops/s` | `143004.898 ops/s` | `-3.75%` | `warning` |
| `segment-index-mixed-drain:partitionedIngestMixed:putWorkload` | `145415.795 ops/s` | `168712.398 ops/s` | `+16.02%` | `better` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed` | `27035.300 ops/s` | `27513.749 ops/s` | `+1.77%` | `neutral` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:getWorkload` | `18410.072 ops/s` | `20495.982 ops/s` | `+11.33%` | `better` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:putWorkload` | `8625.228 ops/s` | `7017.767 ops/s` | `-18.64%` | `worse` |
| `segment-index-persisted-mutation:deleteSync` | `3754.198 ops/s` | `3794.134 ops/s` | `+1.06%` | `neutral` |
| `segment-index-persisted-mutation:putSync` | `3493.112 ops/s` | `3681.528 ops/s` | `+5.39%` | `better` |
