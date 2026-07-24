# Benchmark Comparison

- Bootstrap mode: no prior canonical baseline exists yet, and fallback git baseline predates the benchmark harness.
- Candidate results below are the first canonical benchmark snapshot for this workflow.


- Profile: `segment-index-pr-smoke`
- Baseline SHA: `a5fee006194f67927acecf3435235355f43feac0`
- Candidate SHA: `a5fee006194f67927acecf3435235355f43feac0`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `segment-index-get-live:getHitSync` | `4874031.765 ops/s` | `4874031.765 ops/s` | `+0.00%` | `neutral` |
| `segment-index-get-live:getMissSync` | `4729682.294 ops/s` | `4729682.294 ops/s` | `+0.00%` | `neutral` |
| `segment-index-get-multisegment-hot:getHitSync` | `3466318.370 ops/s` | `3466318.370 ops/s` | `+0.00%` | `neutral` |
| `segment-index-get-multisegment-hot:getMissSync` | `4568840.002 ops/s` | `4568840.002 ops/s` | `+0.00%` | `neutral` |
| `segment-index-get-persisted:getHitSync` | `3593822.346 ops/s` | `3593822.346 ops/s` | `+0.00%` | `neutral` |
| `segment-index-get-persisted:getMissSync` | `4722978.759 ops/s` | `4722978.759 ops/s` | `+0.00%` | `neutral` |
| `segment-index-hot-route-put:putHotRoute` | `4195420.340 ops/s` | `4195420.340 ops/s` | `+0.00%` | `neutral` |
| `segment-index-hot-route-put:putThenGetHotRoute` | `2123054.509 ops/s` | `2123054.509 ops/s` | `+0.00%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed` | `612463.016 ops/s` | `612463.016 ops/s` | `+0.00%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed:getWorkload` | `450429.357 ops/s` | `450429.357 ops/s` | `+0.00%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed:putWorkload` | `162033.658 ops/s` | `162033.658 ops/s` | `+0.00%` | `neutral` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed` | `745875.988 ops/s` | `745875.988 ops/s` | `+0.00%` | `neutral` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:getWorkload` | `732908.061 ops/s` | `732908.061 ops/s` | `+0.00%` | `neutral` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:putWorkload` | `12967.927 ops/s` | `12967.927 ops/s` | `+0.00%` | `neutral` |
| `segment-index-persisted-mutation-concurrent:deleteSync` | `5366.209 ops/s` | `5366.209 ops/s` | `+0.00%` | `neutral` |
| `segment-index-persisted-mutation-concurrent:putSync` | `5349.950 ops/s` | `5349.950 ops/s` | `+0.00%` | `neutral` |
| `segment-index-persisted-mutation:deleteSync` | `2281.038 ops/s` | `2281.038 ops/s` | `+0.00%` | `neutral` |
| `segment-index-persisted-mutation:putSync` | `2273.510 ops/s` | `2273.510 ops/s` | `+0.00%` | `neutral` |
