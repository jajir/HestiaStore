# Benchmark Comparison

- Profile: `segment-index-nightly`
- Baseline SHA: `e898a5940d69a8f46e99d82c0b4f48ffe78308c0`
- Candidate SHA: `67bbf8e34d2b602923270722df932c97760ccab3`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `segment-index-get-live:getHitSync` | `2000805.860 ops/s` | `2268725.857 ops/s` | `+13.39%` | `better` |
| `segment-index-get-live:getMissSync` | `1873895.928 ops/s` | `2026601.922 ops/s` | `+8.15%` | `better` |
| `segment-index-get-persisted:getHitSync` | `1880262.937 ops/s` | `1918602.901 ops/s` | `+2.04%` | `neutral` |
| `segment-index-get-persisted:getMissSync` | `1794271.106 ops/s` | `2059131.767 ops/s` | `+14.76%` | `better` |
| `segment-index-hot-route-put:putHotRoute` | `2178323.646 ops/s` | `2164995.471 ops/s` | `-0.61%` | `neutral` |
| `segment-index-hot-route-put:putThenGetHotRoute` | `1116329.160 ops/s` | `1114403.435 ops/s` | `-0.17%` | `neutral` |
| `segment-index-lifecycle:openAndCheckAndRepairConsistency` | `262.322 ms/op` | `262.592 ms/op` | `+0.10%` | `neutral` |
| `segment-index-lifecycle:openAndCompact` | `283.762 ms/op` | `284.004 ms/op` | `+0.09%` | `neutral` |
| `segment-index-lifecycle:openExisting` | `254.811 ms/op` | `260.260 ms/op` | `+2.14%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed` | `426540.687 ops/s` | `441320.014 ops/s` | `+3.46%` | `better` |
| `segment-index-mixed-drain:partitionedIngestMixed:getWorkload` | `188862.446 ops/s` | `203110.481 ops/s` | `+7.54%` | `better` |
| `segment-index-mixed-drain:partitionedIngestMixed:putWorkload` | `237678.241 ops/s` | `238209.533 ops/s` | `+0.22%` | `neutral` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed` | `870799.700 ops/s` | `936072.873 ops/s` | `+7.50%` | `better` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:getWorkload` | `853711.552 ops/s` | `918267.884 ops/s` | `+7.56%` | `better` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:putWorkload` | `17088.149 ops/s` | `17804.989 ops/s` | `+4.19%` | `better` |
