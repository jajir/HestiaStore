# Benchmark Comparison

- Profile: `segment-index-pr-smoke`
- Baseline SHA: `e0267401c382ba6c3c6bdc8b5961a42a9a7cef02`
- Candidate SHA: `8df8b9848c086730320a4f5a276647c5586602a6`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `segment-index-get-live:getHitSync` | `2791519.244 ops/s` | `3030189.057 ops/s` | `+8.55%` | `better` |
| `segment-index-get-live:getMissSync` | `2773270.275 ops/s` | `2693874.411 ops/s` | `-2.86%` | `neutral` |
| `segment-index-get-persisted:getHitSync` | `2456831.340 ops/s` | `2406182.471 ops/s` | `-2.06%` | `neutral` |
| `segment-index-get-persisted:getMissSync` | `3055005.229 ops/s` | `2765022.572 ops/s` | `-9.49%` | `worse` |
| `segment-index-hot-route-put:putHotRoute` | `2778235.687 ops/s` | `2862039.656 ops/s` | `+3.02%` | `better` |
| `segment-index-hot-route-put:putThenGetHotRoute` | `1421740.524 ops/s` | `1479107.114 ops/s` | `+4.03%` | `better` |
| `segment-index-mixed-drain:partitionedIngestMixed` | `-` | `550430.572 ops/s` | `-` | `new` |
| `segment-index-mixed-drain:partitionedIngestMixed:getWorkload` | `-` | `318033.123 ops/s` | `-` | `new` |
| `segment-index-mixed-drain:partitionedIngestMixed:putWorkload` | `-` | `232397.449 ops/s` | `-` | `new` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed` | `-` | `989078.109 ops/s` | `-` | `new` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:getWorkload` | `-` | `971245.840 ops/s` | `-` | `new` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:putWorkload` | `-` | `17832.270 ops/s` | `-` | `new` |
