# Benchmark Comparison

- Bootstrap mode: no prior canonical baseline exists yet, and fallback git baseline predates the benchmark harness.
- Candidate results below are the first canonical benchmark snapshot for this workflow.


- Profile: `segment-index-pr-smoke`
- Baseline SHA: `4500b7c559212125ae098ac65fc86bb332319dfe`
- Candidate SHA: `4500b7c559212125ae098ac65fc86bb332319dfe`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `segment-index-get-live:getHitSync` | `2688956.459 ops/s` | `2688956.459 ops/s` | `+0.00%` | `neutral` |
| `segment-index-get-live:getMissSync` | `2506286.063 ops/s` | `2506286.063 ops/s` | `+0.00%` | `neutral` |
| `segment-index-get-multisegment-hot:getHitSync` | `1924877.930 ops/s` | `1924877.930 ops/s` | `+0.00%` | `neutral` |
| `segment-index-get-multisegment-hot:getMissSync` | `2590731.345 ops/s` | `2590731.345 ops/s` | `+0.00%` | `neutral` |
| `segment-index-get-persisted:getHitSync` | `1995050.396 ops/s` | `1995050.396 ops/s` | `+0.00%` | `neutral` |
| `segment-index-get-persisted:getMissSync` | `2657340.212 ops/s` | `2657340.212 ops/s` | `+0.00%` | `neutral` |
| `segment-index-hot-route-put:putHotRoute` | `2406300.538 ops/s` | `2406300.538 ops/s` | `+0.00%` | `neutral` |
| `segment-index-hot-route-put:putThenGetHotRoute` | `1331650.199 ops/s` | `1331650.199 ops/s` | `+0.00%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed` | `509352.215 ops/s` | `509352.215 ops/s` | `+0.00%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed:getWorkload` | `319757.227 ops/s` | `319757.227 ops/s` | `+0.00%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed:putWorkload` | `189594.988 ops/s` | `189594.988 ops/s` | `+0.00%` | `neutral` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed` | `795949.204 ops/s` | `795949.204 ops/s` | `+0.00%` | `neutral` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:getWorkload` | `780776.584 ops/s` | `780776.584 ops/s` | `+0.00%` | `neutral` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:putWorkload` | `15172.620 ops/s` | `15172.620 ops/s` | `+0.00%` | `neutral` |
| `segment-index-persisted-mutation-concurrent:deleteSync` | `3371.719 ops/s` | `3371.719 ops/s` | `+0.00%` | `neutral` |
| `segment-index-persisted-mutation-concurrent:putSync` | `3172.692 ops/s` | `3172.692 ops/s` | `+0.00%` | `neutral` |
| `segment-index-persisted-mutation:deleteSync` | `1782.382 ops/s` | `1782.382 ops/s` | `+0.00%` | `neutral` |
| `segment-index-persisted-mutation:putSync` | `1949.830 ops/s` | `1949.830 ops/s` | `+0.00%` | `neutral` |
| `segment-index-range-scan:boundedScan` | `24.886 us/op` | `24.886 us/op` | `+0.00%` | `neutral` |
| `segment-index-range-scan:fullStreamRangeFallback` | `1911.571 us/op` | `1911.571 us/op` | `+0.00%` | `neutral` |
| `segment-index-range-scan:sequentialRead` | `3565.289 us/op` | `3565.289 us/op` | `+0.00%` | `neutral` |
| `segment-merge-sequential:mergeSequential` | `307.423 us/op` | `307.423 us/op` | `+0.00%` | `neutral` |
