# Benchmark Comparison

- Bootstrap mode: no prior canonical baseline exists yet, and fallback git baseline predates the benchmark harness.
- Candidate results below are the first canonical benchmark snapshot for this workflow.


- Profile: `segment-index-pr-smoke`
- Baseline SHA: `6802a1ecd057ef44daba17179d663834b289f16a`
- Candidate SHA: `6802a1ecd057ef44daba17179d663834b289f16a`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `segment-index-get-live:getHitSync` | `4799263.386 ops/s` | `4799263.386 ops/s` | `+0.00%` | `neutral` |
| `segment-index-get-live:getMissSync` | `4381866.561 ops/s` | `4381866.561 ops/s` | `+0.00%` | `neutral` |
| `segment-index-get-multisegment-hot:getHitSync` | `3280479.027 ops/s` | `3280479.027 ops/s` | `+0.00%` | `neutral` |
| `segment-index-get-multisegment-hot:getMissSync` | `4434571.463 ops/s` | `4434571.463 ops/s` | `+0.00%` | `neutral` |
| `segment-index-get-persisted:getHitSync` | `3437513.061 ops/s` | `3437513.061 ops/s` | `+0.00%` | `neutral` |
| `segment-index-get-persisted:getMissSync` | `4317225.295 ops/s` | `4317225.295 ops/s` | `+0.00%` | `neutral` |
| `segment-index-hot-route-put:putHotRoute` | `3980864.571 ops/s` | `3980864.571 ops/s` | `+0.00%` | `neutral` |
| `segment-index-hot-route-put:putThenGetHotRoute` | `2082054.860 ops/s` | `2082054.860 ops/s` | `+0.00%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed` | `583547.567 ops/s` | `583547.567 ops/s` | `+0.00%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed:getWorkload` | `414579.786 ops/s` | `414579.786 ops/s` | `+0.00%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed:putWorkload` | `168967.781 ops/s` | `168967.781 ops/s` | `+0.00%` | `neutral` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed` | `811158.630 ops/s` | `811158.630 ops/s` | `+0.00%` | `neutral` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:getWorkload` | `797465.345 ops/s` | `797465.345 ops/s` | `+0.00%` | `neutral` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:putWorkload` | `13693.285 ops/s` | `13693.285 ops/s` | `+0.00%` | `neutral` |
| `segment-index-persisted-mutation-concurrent:deleteSync` | `7241.097 ops/s` | `7241.097 ops/s` | `+0.00%` | `neutral` |
| `segment-index-persisted-mutation-concurrent:putSync` | `7342.690 ops/s` | `7342.690 ops/s` | `+0.00%` | `neutral` |
| `segment-index-persisted-mutation:deleteSync` | `3345.246 ops/s` | `3345.246 ops/s` | `+0.00%` | `neutral` |
| `segment-index-persisted-mutation:putSync` | `3399.733 ops/s` | `3399.733 ops/s` | `+0.00%` | `neutral` |
