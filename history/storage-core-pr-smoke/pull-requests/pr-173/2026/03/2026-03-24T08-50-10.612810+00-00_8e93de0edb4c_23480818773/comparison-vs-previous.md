# Benchmark Comparison

- Bootstrap mode: no prior canonical baseline exists yet, and fallback git baseline predates the benchmark harness.
- Candidate results below are the first canonical benchmark snapshot for this workflow.


- Profile: `storage-core-pr-smoke`
- Baseline SHA: `8e93de0edb4cf85b6be304f6d6ddd4df7921d6fb`
- Candidate SHA: `8e93de0edb4cf85b6be304f6d6ddd4df7921d6fb`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `single-chunk-entry-write:writeEntrySteadyState` | `7819438.857 ops/s` | `7819438.857 ops/s` | `+0.00%` | `neutral` |
| `sorted-data-diff-key-read:readNextKey` | `6709511.297 ops/s` | `6709511.297 ops/s` | `+0.00%` | `neutral` |
