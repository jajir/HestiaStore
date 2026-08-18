# Benchmark Comparison

- Bootstrap mode: no prior canonical baseline exists yet, and fallback git baseline predates the benchmark harness.
- Candidate results below are the first canonical benchmark snapshot for this workflow.


- Profile: `segment-index-pr-smoke`
- Baseline SHA: `62857650a440b41a704c4f35bea848cdd5abdf78`
- Candidate SHA: `62857650a440b41a704c4f35bea848cdd5abdf78`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `segment-index-get-live:getHitSync` | `4890209.027 ops/s` | `4890209.027 ops/s` | `+0.00%` | `neutral` |
| `segment-index-get-live:getMissSync` | `4591531.696 ops/s` | `4591531.696 ops/s` | `+0.00%` | `neutral` |
| `segment-index-get-multisegment-hot:getHitSync` | `3570338.800 ops/s` | `3570338.800 ops/s` | `+0.00%` | `neutral` |
| `segment-index-get-multisegment-hot:getMissSync` | `4900139.350 ops/s` | `4900139.350 ops/s` | `+0.00%` | `neutral` |
| `segment-index-get-persisted:getHitSync` | `3409116.571 ops/s` | `3409116.571 ops/s` | `+0.00%` | `neutral` |
| `segment-index-get-persisted:getMissSync` | `4855113.421 ops/s` | `4855113.421 ops/s` | `+0.00%` | `neutral` |
| `segment-index-hot-route-put:putHotRoute` | `4424925.791 ops/s` | `4424925.791 ops/s` | `+0.00%` | `neutral` |
| `segment-index-hot-route-put:putThenGetHotRoute` | `2194943.317 ops/s` | `2194943.317 ops/s` | `+0.00%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed` | `530183.954 ops/s` | `530183.954 ops/s` | `+0.00%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed:getWorkload` | `364380.352 ops/s` | `364380.352 ops/s` | `+0.00%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed:putWorkload` | `165803.602 ops/s` | `165803.602 ops/s` | `+0.00%` | `neutral` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed` | `764304.003 ops/s` | `764304.003 ops/s` | `+0.00%` | `neutral` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:getWorkload` | `745916.946 ops/s` | `745916.946 ops/s` | `+0.00%` | `neutral` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:putWorkload` | `18387.057 ops/s` | `18387.057 ops/s` | `+0.00%` | `neutral` |
| `segment-index-persisted-mutation-concurrent:deleteSync` | `6247.600 ops/s` | `6247.600 ops/s` | `+0.00%` | `neutral` |
| `segment-index-persisted-mutation-concurrent:putSync` | `6458.746 ops/s` | `6458.746 ops/s` | `+0.00%` | `neutral` |
| `segment-index-persisted-mutation:deleteSync` | `2565.293 ops/s` | `2565.293 ops/s` | `+0.00%` | `neutral` |
| `segment-index-persisted-mutation:putSync` | `2554.685 ops/s` | `2554.685 ops/s` | `+0.00%` | `neutral` |
| `segment-index-range-scan:boundedScan` | `28.811 us/op` | `28.811 us/op` | `+0.00%` | `neutral` |
| `segment-index-range-scan:fullStreamRangeFallback` | `2087.300 us/op` | `2087.300 us/op` | `+0.00%` | `neutral` |
| `segment-index-range-scan:sequentialRead` | `3931.138 us/op` | `3931.138 us/op` | `+0.00%` | `neutral` |
| `segment-merge-sequential:mergeSequential` | `323.277 us/op` | `323.277 us/op` | `+0.00%` | `neutral` |
