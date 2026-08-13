# Benchmark Comparison

- Profile: `segment-index-pr-smoke`
- Baseline SHA: `4500b7c559212125ae098ac65fc86bb332319dfe`
- Candidate SHA: `ddbfc586465bac0fdf701cde13f5f54ca704f4d4`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `segment-index-get-live:getHitSync` | `2688956.459 ops/s` | `4996543.899 ops/s` | `+85.82%` | `better` |
| `segment-index-get-live:getMissSync` | `2506286.063 ops/s` | `4226624.478 ops/s` | `+68.64%` | `better` |
| `segment-index-get-multisegment-hot:getHitSync` | `1924877.930 ops/s` | `3404069.180 ops/s` | `+76.85%` | `better` |
| `segment-index-get-multisegment-hot:getMissSync` | `2590731.345 ops/s` | `4245505.969 ops/s` | `+63.87%` | `better` |
| `segment-index-get-persisted:getHitSync` | `1995050.396 ops/s` | `3573738.689 ops/s` | `+79.13%` | `better` |
| `segment-index-get-persisted:getMissSync` | `2657340.212 ops/s` | `4682366.271 ops/s` | `+76.20%` | `better` |
| `segment-index-hot-route-put:putHotRoute` | `2406300.538 ops/s` | `4325424.782 ops/s` | `+79.75%` | `better` |
| `segment-index-hot-route-put:putThenGetHotRoute` | `1331650.199 ops/s` | `2299422.120 ops/s` | `+72.67%` | `better` |
| `segment-index-mixed-drain:partitionedIngestMixed` | `509352.215 ops/s` | `509745.572 ops/s` | `+0.08%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed:getWorkload` | `319757.227 ops/s` | `323380.707 ops/s` | `+1.13%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed:putWorkload` | `189594.988 ops/s` | `186364.866 ops/s` | `-1.70%` | `neutral` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed` | `795949.204 ops/s` | `846865.738 ops/s` | `+6.40%` | `better` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:getWorkload` | `780776.584 ops/s` | `828746.241 ops/s` | `+6.14%` | `better` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:putWorkload` | `15172.620 ops/s` | `18119.497 ops/s` | `+19.42%` | `better` |
| `segment-index-persisted-mutation-concurrent:deleteSync` | `3371.719 ops/s` | `7212.531 ops/s` | `+113.91%` | `better` |
| `segment-index-persisted-mutation-concurrent:putSync` | `3172.692 ops/s` | `7169.565 ops/s` | `+125.98%` | `better` |
| `segment-index-persisted-mutation:deleteSync` | `1782.382 ops/s` | `2899.565 ops/s` | `+62.68%` | `better` |
| `segment-index-persisted-mutation:putSync` | `1949.830 ops/s` | `2784.601 ops/s` | `+42.81%` | `better` |
| `segment-index-range-scan:boundedScan` | `24.886 us/op` | `33.473 us/op` | `+34.50%` | `better` |
| `segment-index-range-scan:fullStreamRangeFallback` | `1911.571 us/op` | `2090.206 us/op` | `+9.34%` | `better` |
| `segment-index-range-scan:sequentialRead` | `3565.289 us/op` | `4048.816 us/op` | `+13.56%` | `better` |
| `segment-merge-sequential:mergeSequential` | `307.423 us/op` | `324.769 us/op` | `+5.64%` | `better` |
