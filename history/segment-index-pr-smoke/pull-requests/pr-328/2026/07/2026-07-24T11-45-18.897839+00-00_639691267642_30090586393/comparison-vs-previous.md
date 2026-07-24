# Benchmark Comparison

- Bootstrap mode: no prior canonical baseline exists yet, and fallback git baseline predates the benchmark harness.
- Candidate results below are the first canonical benchmark snapshot for this workflow.


- Profile: `segment-index-pr-smoke`
- Baseline SHA: `63969126764235557acd7c18485700091cb52fdd`
- Candidate SHA: `63969126764235557acd7c18485700091cb52fdd`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `segment-index-get-live:getHitSync` | `4501822.768 ops/s` | `4501822.768 ops/s` | `+0.00%` | `neutral` |
| `segment-index-get-live:getMissSync` | `4427676.669 ops/s` | `4427676.669 ops/s` | `+0.00%` | `neutral` |
| `segment-index-get-multisegment-hot:getHitSync` | `3544001.445 ops/s` | `3544001.445 ops/s` | `+0.00%` | `neutral` |
| `segment-index-get-multisegment-hot:getMissSync` | `4178847.437 ops/s` | `4178847.437 ops/s` | `+0.00%` | `neutral` |
| `segment-index-get-persisted:getHitSync` | `3374562.916 ops/s` | `3374562.916 ops/s` | `+0.00%` | `neutral` |
| `segment-index-get-persisted:getMissSync` | `4317575.875 ops/s` | `4317575.875 ops/s` | `+0.00%` | `neutral` |
| `segment-index-hot-route-put:putHotRoute` | `3883880.963 ops/s` | `3883880.963 ops/s` | `+0.00%` | `neutral` |
| `segment-index-hot-route-put:putThenGetHotRoute` | `2001284.607 ops/s` | `2001284.607 ops/s` | `+0.00%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed` | `602919.260 ops/s` | `602919.260 ops/s` | `+0.00%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed:getWorkload` | `450796.786 ops/s` | `450796.786 ops/s` | `+0.00%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed:putWorkload` | `152122.474 ops/s` | `152122.474 ops/s` | `+0.00%` | `neutral` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed` | `660160.539 ops/s` | `660160.539 ops/s` | `+0.00%` | `neutral` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:getWorkload` | `645610.182 ops/s` | `645610.182 ops/s` | `+0.00%` | `neutral` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:putWorkload` | `14550.358 ops/s` | `14550.358 ops/s` | `+0.00%` | `neutral` |
| `segment-index-persisted-mutation-concurrent:deleteSync` | `6342.841 ops/s` | `6342.841 ops/s` | `+0.00%` | `neutral` |
| `segment-index-persisted-mutation-concurrent:putSync` | `6592.411 ops/s` | `6592.411 ops/s` | `+0.00%` | `neutral` |
| `segment-index-persisted-mutation:deleteSync` | `2566.116 ops/s` | `2566.116 ops/s` | `+0.00%` | `neutral` |
| `segment-index-persisted-mutation:putSync` | `2559.728 ops/s` | `2559.728 ops/s` | `+0.00%` | `neutral` |
