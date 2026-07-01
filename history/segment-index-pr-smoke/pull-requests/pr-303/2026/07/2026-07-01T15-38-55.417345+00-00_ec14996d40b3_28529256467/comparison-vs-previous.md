# Benchmark Comparison

- Profile: `segment-index-pr-smoke`
- Baseline SHA: `67bbf8e34d2b602923270722df932c97760ccab3`
- Candidate SHA: `ec14996d40b381e174a01cfb716e6b8f52956f1f`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `segment-index-get-live:getHitSync` | `3004039.524 ops/s` | `2705644.298 ops/s` | `-9.93%` | `worse` |
| `segment-index-get-live:getMissSync` | `2604262.032 ops/s` | `2581338.826 ops/s` | `-0.88%` | `neutral` |
| `segment-index-get-persisted:getHitSync` | `2545356.489 ops/s` | `2525148.603 ops/s` | `-0.79%` | `neutral` |
| `segment-index-get-persisted:getMissSync` | `2735188.496 ops/s` | `2608960.780 ops/s` | `-4.61%` | `warning` |
| `segment-index-hot-route-put:putHotRoute` | `2773705.140 ops/s` | `2767102.108 ops/s` | `-0.24%` | `neutral` |
| `segment-index-hot-route-put:putThenGetHotRoute` | `1474387.389 ops/s` | `1395417.222 ops/s` | `-5.36%` | `warning` |
| `segment-index-mixed-drain:partitionedIngestMixed` | `529385.430 ops/s` | `-` | `-` | `removed` |
| `segment-index-mixed-drain:partitionedIngestMixed:getWorkload` | `318165.871 ops/s` | `-` | `-` | `removed` |
| `segment-index-mixed-drain:partitionedIngestMixed:putWorkload` | `211219.560 ops/s` | `-` | `-` | `removed` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed` | `952971.643 ops/s` | `-` | `-` | `removed` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:getWorkload` | `932558.875 ops/s` | `-` | `-` | `removed` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:putWorkload` | `20412.768 ops/s` | `-` | `-` | `removed` |
