# Benchmark Comparison

- Bootstrap mode: no prior canonical baseline exists yet, and fallback git baseline predates the benchmark harness.
- Candidate results below are the first canonical benchmark snapshot for this workflow.


- Profile: `segment-index-pr-smoke`
- Baseline SHA: `c0ab96dedfed051af6401d8322b0e7c046e10a49`
- Candidate SHA: `c0ab96dedfed051af6401d8322b0e7c046e10a49`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `segment-index-get-live:getHitSync` | `2530263.942 ops/s` | `2530263.942 ops/s` | `+0.00%` | `neutral` |
| `segment-index-get-live:getMissSync` | `2739626.310 ops/s` | `2739626.310 ops/s` | `+0.00%` | `neutral` |
| `segment-index-get-multisegment-hot:getHitSync` | `1969403.324 ops/s` | `1969403.324 ops/s` | `+0.00%` | `neutral` |
| `segment-index-get-multisegment-hot:getMissSync` | `2842652.580 ops/s` | `2842652.580 ops/s` | `+0.00%` | `neutral` |
| `segment-index-get-persisted:getHitSync` | `2073945.683 ops/s` | `2073945.683 ops/s` | `+0.00%` | `neutral` |
| `segment-index-get-persisted:getMissSync` | `2964942.676 ops/s` | `2964942.676 ops/s` | `+0.00%` | `neutral` |
| `segment-index-hot-route-put:putHotRoute` | `2479638.879 ops/s` | `2479638.879 ops/s` | `+0.00%` | `neutral` |
| `segment-index-hot-route-put:putThenGetHotRoute` | `1343640.151 ops/s` | `1343640.151 ops/s` | `+0.00%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed` | `469205.336 ops/s` | `469205.336 ops/s` | `+0.00%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed:getWorkload` | `281163.577 ops/s` | `281163.577 ops/s` | `+0.00%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed:putWorkload` | `188041.758 ops/s` | `188041.758 ops/s` | `+0.00%` | `neutral` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed` | `794823.360 ops/s` | `794823.360 ops/s` | `+0.00%` | `neutral` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:getWorkload` | `779877.493 ops/s` | `779877.493 ops/s` | `+0.00%` | `neutral` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:putWorkload` | `14945.867 ops/s` | `14945.867 ops/s` | `+0.00%` | `neutral` |
| `segment-index-persisted-mutation-concurrent:deleteSync` | `4257.990 ops/s` | `4257.990 ops/s` | `+0.00%` | `neutral` |
| `segment-index-persisted-mutation-concurrent:putSync` | `3839.772 ops/s` | `3839.772 ops/s` | `+0.00%` | `neutral` |
| `segment-index-persisted-mutation:deleteSync` | `1973.337 ops/s` | `1973.337 ops/s` | `+0.00%` | `neutral` |
| `segment-index-persisted-mutation:putSync` | `1827.851 ops/s` | `1827.851 ops/s` | `+0.00%` | `neutral` |
| `segment-index-range-scan:boundedScan` | `23.703 us/op` | `23.703 us/op` | `+0.00%` | `neutral` |
| `segment-index-range-scan:fullStreamRangeFallback` | `1866.270 us/op` | `1866.270 us/op` | `+0.00%` | `neutral` |
| `segment-index-range-scan:sequentialRead` | `3546.753 us/op` | `3546.753 us/op` | `+0.00%` | `neutral` |
| `segment-merge-sequential:mergeSequential` | `300.397 us/op` | `300.397 us/op` | `+0.00%` | `neutral` |
