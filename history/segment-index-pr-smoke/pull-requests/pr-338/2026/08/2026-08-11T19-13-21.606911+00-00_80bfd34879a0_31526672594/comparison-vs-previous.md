# Benchmark Comparison

- Bootstrap mode: no prior canonical baseline exists yet, and fallback git baseline predates the benchmark harness.
- Candidate results below are the first canonical benchmark snapshot for this workflow.


- Profile: `segment-index-pr-smoke`
- Baseline SHA: `80bfd34879a03e9bba487c6095a40aaeb03a4b67`
- Candidate SHA: `80bfd34879a03e9bba487c6095a40aaeb03a4b67`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `segment-index-get-live:getHitSync` | `4730080.736 ops/s` | `4730080.736 ops/s` | `+0.00%` | `neutral` |
| `segment-index-get-live:getMissSync` | `4766470.704 ops/s` | `4766470.704 ops/s` | `+0.00%` | `neutral` |
| `segment-index-get-multisegment-hot:getHitSync` | `3325675.661 ops/s` | `3325675.661 ops/s` | `+0.00%` | `neutral` |
| `segment-index-get-multisegment-hot:getMissSync` | `4471706.174 ops/s` | `4471706.174 ops/s` | `+0.00%` | `neutral` |
| `segment-index-get-persisted:getHitSync` | `3396350.030 ops/s` | `3396350.030 ops/s` | `+0.00%` | `neutral` |
| `segment-index-get-persisted:getMissSync` | `4670565.370 ops/s` | `4670565.370 ops/s` | `+0.00%` | `neutral` |
| `segment-index-hot-route-put:putHotRoute` | `3837848.846 ops/s` | `3837848.846 ops/s` | `+0.00%` | `neutral` |
| `segment-index-hot-route-put:putThenGetHotRoute` | `2067126.502 ops/s` | `2067126.502 ops/s` | `+0.00%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed` | `537142.916 ops/s` | `537142.916 ops/s` | `+0.00%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed:getWorkload` | `376046.429 ops/s` | `376046.429 ops/s` | `+0.00%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed:putWorkload` | `161096.487 ops/s` | `161096.487 ops/s` | `+0.00%` | `neutral` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed` | `859922.781 ops/s` | `859922.781 ops/s` | `+0.00%` | `neutral` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:getWorkload` | `845256.982 ops/s` | `845256.982 ops/s` | `+0.00%` | `neutral` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:putWorkload` | `14665.799 ops/s` | `14665.799 ops/s` | `+0.00%` | `neutral` |
| `segment-index-persisted-mutation-concurrent:deleteSync` | `7487.357 ops/s` | `7487.357 ops/s` | `+0.00%` | `neutral` |
| `segment-index-persisted-mutation-concurrent:putSync` | `7295.702 ops/s` | `7295.702 ops/s` | `+0.00%` | `neutral` |
| `segment-index-persisted-mutation:deleteSync` | `3274.525 ops/s` | `3274.525 ops/s` | `+0.00%` | `neutral` |
| `segment-index-persisted-mutation:putSync` | `3273.228 ops/s` | `3273.228 ops/s` | `+0.00%` | `neutral` |
| `segment-index-range-scan:boundedScan` | `34.576 us/op` | `34.576 us/op` | `+0.00%` | `neutral` |
| `segment-index-range-scan:fullStreamRangeFallback` | `2282.422 us/op` | `2282.422 us/op` | `+0.00%` | `neutral` |
| `segment-index-range-scan:sequentialRead` | `4273.790 us/op` | `4273.790 us/op` | `+0.00%` | `neutral` |
| `segment-merge-sequential:mergeSequential` | `349.670 us/op` | `349.670 us/op` | `+0.00%` | `neutral` |
