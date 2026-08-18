# Benchmark Comparison

- Bootstrap mode: no prior canonical baseline exists yet, and fallback git baseline predates the benchmark harness.
- Candidate results below are the first canonical benchmark snapshot for this workflow.


- Profile: `segment-index-pr-smoke`
- Baseline SHA: `bf830eb6be92f45201d79a8df2c45b2570dec148`
- Candidate SHA: `bf830eb6be92f45201d79a8df2c45b2570dec148`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `segment-index-get-live:getHitSync` | `4930000.319 ops/s` | `4930000.319 ops/s` | `+0.00%` | `neutral` |
| `segment-index-get-live:getMissSync` | `4863914.805 ops/s` | `4863914.805 ops/s` | `+0.00%` | `neutral` |
| `segment-index-get-multisegment-hot:getHitSync` | `3338864.105 ops/s` | `3338864.105 ops/s` | `+0.00%` | `neutral` |
| `segment-index-get-multisegment-hot:getMissSync` | `4553286.052 ops/s` | `4553286.052 ops/s` | `+0.00%` | `neutral` |
| `segment-index-get-persisted:getHitSync` | `3300569.917 ops/s` | `3300569.917 ops/s` | `+0.00%` | `neutral` |
| `segment-index-get-persisted:getMissSync` | `4687755.521 ops/s` | `4687755.521 ops/s` | `+0.00%` | `neutral` |
| `segment-index-hot-route-put:putHotRoute` | `4275082.908 ops/s` | `4275082.908 ops/s` | `+0.00%` | `neutral` |
| `segment-index-hot-route-put:putThenGetHotRoute` | `2179288.776 ops/s` | `2179288.776 ops/s` | `+0.00%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed` | `600171.571 ops/s` | `600171.571 ops/s` | `+0.00%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed:getWorkload` | `426096.053 ops/s` | `426096.053 ops/s` | `+0.00%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed:putWorkload` | `174075.517 ops/s` | `174075.517 ops/s` | `+0.00%` | `neutral` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed` | `866491.887 ops/s` | `866491.887 ops/s` | `+0.00%` | `neutral` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:getWorkload` | `848408.324 ops/s` | `848408.324 ops/s` | `+0.00%` | `neutral` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:putWorkload` | `18083.563 ops/s` | `18083.563 ops/s` | `+0.00%` | `neutral` |
| `segment-index-persisted-mutation-concurrent:deleteSync` | `6226.840 ops/s` | `6226.840 ops/s` | `+0.00%` | `neutral` |
| `segment-index-persisted-mutation-concurrent:putSync` | `6249.130 ops/s` | `6249.130 ops/s` | `+0.00%` | `neutral` |
| `segment-index-persisted-mutation:deleteSync` | `2313.217 ops/s` | `2313.217 ops/s` | `+0.00%` | `neutral` |
| `segment-index-persisted-mutation:putSync` | `2461.051 ops/s` | `2461.051 ops/s` | `+0.00%` | `neutral` |
| `segment-index-range-scan:boundedScan` | `32.244 us/op` | `32.244 us/op` | `+0.00%` | `neutral` |
| `segment-index-range-scan:fullStreamRangeFallback` | `2148.703 us/op` | `2148.703 us/op` | `+0.00%` | `neutral` |
| `segment-index-range-scan:sequentialRead` | `4590.205 us/op` | `4590.205 us/op` | `+0.00%` | `neutral` |
| `segment-merge-sequential:mergeSequential` | `324.794 us/op` | `324.794 us/op` | `+0.00%` | `neutral` |
