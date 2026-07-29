# Benchmark Comparison

- Bootstrap mode: no prior canonical baseline exists yet, and fallback git baseline predates the benchmark harness.
- Candidate results below are the first canonical benchmark snapshot for this workflow.


- Profile: `segment-index-pr-smoke`
- Baseline SHA: `05cd382b2563f3e80d5bffc6455ebc2c162d5f33`
- Candidate SHA: `05cd382b2563f3e80d5bffc6455ebc2c162d5f33`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `segment-index-get-live:getHitSync` | `4817809.106 ops/s` | `4817809.106 ops/s` | `+0.00%` | `neutral` |
| `segment-index-get-live:getMissSync` | `4782910.431 ops/s` | `4782910.431 ops/s` | `+0.00%` | `neutral` |
| `segment-index-get-multisegment-hot:getHitSync` | `3660852.171 ops/s` | `3660852.171 ops/s` | `+0.00%` | `neutral` |
| `segment-index-get-multisegment-hot:getMissSync` | `4458059.332 ops/s` | `4458059.332 ops/s` | `+0.00%` | `neutral` |
| `segment-index-get-persisted:getHitSync` | `3269042.470 ops/s` | `3269042.470 ops/s` | `+0.00%` | `neutral` |
| `segment-index-get-persisted:getMissSync` | `4954424.376 ops/s` | `4954424.376 ops/s` | `+0.00%` | `neutral` |
| `segment-index-hot-route-put:putHotRoute` | `4275295.926 ops/s` | `4275295.926 ops/s` | `+0.00%` | `neutral` |
| `segment-index-hot-route-put:putThenGetHotRoute` | `2222099.972 ops/s` | `2222099.972 ops/s` | `+0.00%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed` | `566822.271 ops/s` | `566822.271 ops/s` | `+0.00%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed:getWorkload` | `405526.856 ops/s` | `405526.856 ops/s` | `+0.00%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed:putWorkload` | `161295.415 ops/s` | `161295.415 ops/s` | `+0.00%` | `neutral` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed` | `592425.402 ops/s` | `592425.402 ops/s` | `+0.00%` | `neutral` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:getWorkload` | `579663.413 ops/s` | `579663.413 ops/s` | `+0.00%` | `neutral` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:putWorkload` | `12761.988 ops/s` | `12761.988 ops/s` | `+0.00%` | `neutral` |
| `segment-index-persisted-mutation-concurrent:deleteSync` | `6064.402 ops/s` | `6064.402 ops/s` | `+0.00%` | `neutral` |
| `segment-index-persisted-mutation-concurrent:putSync` | `6313.803 ops/s` | `6313.803 ops/s` | `+0.00%` | `neutral` |
| `segment-index-persisted-mutation:deleteSync` | `2389.505 ops/s` | `2389.505 ops/s` | `+0.00%` | `neutral` |
| `segment-index-persisted-mutation:putSync` | `2482.660 ops/s` | `2482.660 ops/s` | `+0.00%` | `neutral` |
| `segment-index-range-scan:boundedScan` | `29.853 us/op` | `29.853 us/op` | `+0.00%` | `neutral` |
| `segment-index-range-scan:fullStreamRangeFallback` | `2139.266 us/op` | `2139.266 us/op` | `+0.00%` | `neutral` |
| `segment-index-range-scan:sequentialRead` | `4166.802 us/op` | `4166.802 us/op` | `+0.00%` | `neutral` |
| `segment-merge-sequential:mergeSequential` | `323.832 us/op` | `323.832 us/op` | `+0.00%` | `neutral` |
