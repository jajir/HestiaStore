# Benchmark Comparison

- Profile: `segment-index-pr-smoke`
- Baseline SHA: `30f93a9be86df7361bbeb5cf93c3547cbb155d00`
- Candidate SHA: `87f480af58bc3f79fba7de56b9997fce94f6ab7e`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `segment-index-get-live:getHitSync` | `3521665.973 ops/s` | `3663704.648 ops/s` | `+4.03%` | `better` |
| `segment-index-get-live:getMissSync` | `3960477.744 ops/s` | `4048662.158 ops/s` | `+2.23%` | `neutral` |
| `segment-index-get-multisegment-hot:getHitSync` | `95.791 ops/s` | `93.526 ops/s` | `-2.37%` | `neutral` |
| `segment-index-get-multisegment-hot:getMissSync` | `3881002.276 ops/s` | `4177083.126 ops/s` | `+7.63%` | `better` |
| `segment-index-get-persisted:getHitSync` | `110722.687 ops/s` | `111485.692 ops/s` | `+0.69%` | `neutral` |
| `segment-index-get-persisted:getMissSync` | `4146460.186 ops/s` | `3973717.172 ops/s` | `-4.17%` | `warning` |
| `segment-index-hot-route-put:putHotRoute` | `2439368.610 ops/s` | `2182643.538 ops/s` | `-10.52%` | `worse` |
| `segment-index-hot-route-put:putThenGetHotRoute` | `1491170.169 ops/s` | `1481396.843 ops/s` | `-0.66%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed` | `346328.290 ops/s` | `342633.135 ops/s` | `-1.07%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed:getWorkload` | `186561.508 ops/s` | `173368.092 ops/s` | `-7.07%` | `worse` |
| `segment-index-mixed-drain:partitionedIngestMixed:putWorkload` | `159766.782 ops/s` | `169265.043 ops/s` | `+5.95%` | `better` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed` | `34202.411 ops/s` | `51153.547 ops/s` | `+49.56%` | `better` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:getWorkload` | `27864.369 ops/s` | `43378.968 ops/s` | `+55.68%` | `better` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:putWorkload` | `6338.043 ops/s` | `7774.578 ops/s` | `+22.67%` | `better` |
| `segment-index-persisted-mutation:deleteSync` | `2483.335 ops/s` | `2577.431 ops/s` | `+3.79%` | `better` |
| `segment-index-persisted-mutation:putSync` | `2396.452 ops/s` | `2488.796 ops/s` | `+3.85%` | `better` |
