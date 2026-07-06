# Benchmark Comparison

- Profile: `segment-index-pr-smoke`
- Baseline SHA: `e0267401c382ba6c3c6bdc8b5961a42a9a7cef02`
- Candidate SHA: `1c3faa27924d17d9c79d570ba54348d57ee2e408`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `segment-index-get-live:getHitSync` | `2239714.325 ops/s` | `2265465.056 ops/s` | `+1.15%` | `neutral` |
| `segment-index-get-live:getMissSync` | `2357478.320 ops/s` | `2318101.967 ops/s` | `-1.67%` | `neutral` |
| `segment-index-get-persisted:getHitSync` | `2152876.131 ops/s` | `1908628.872 ops/s` | `-11.35%` | `worse` |
| `segment-index-get-persisted:getMissSync` | `2241865.876 ops/s` | `2150772.779 ops/s` | `-4.06%` | `warning` |
| `segment-index-hot-route-put:putHotRoute` | `2200765.912 ops/s` | `2238250.050 ops/s` | `+1.70%` | `neutral` |
| `segment-index-hot-route-put:putThenGetHotRoute` | `1098838.611 ops/s` | `1119625.235 ops/s` | `+1.89%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed` | `-` | `464849.791 ops/s` | `-` | `new` |
| `segment-index-mixed-drain:partitionedIngestMixed:getWorkload` | `-` | `316879.955 ops/s` | `-` | `new` |
| `segment-index-mixed-drain:partitionedIngestMixed:putWorkload` | `-` | `147969.836 ops/s` | `-` | `new` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed` | `376957.720 ops/s` | `631885.742 ops/s` | `+67.63%` | `better` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:getWorkload` | `365611.520 ops/s` | `616702.468 ops/s` | `+68.68%` | `better` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:putWorkload` | `11346.200 ops/s` | `15183.274 ops/s` | `+33.82%` | `better` |
