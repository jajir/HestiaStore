# Benchmark Comparison

- Profile: `segment-index-pr-smoke`
- Baseline SHA: `e65add82b78aa708740ea09faa5b47e2f6dcb992`
- Candidate SHA: `a232ff96e55757742a257a30e43a12f29ad641bd`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `segment-index-get-live:getHitSync` | `2282783.933 ops/s` | `2326506.474 ops/s` | `+1.92%` | `neutral` |
| `segment-index-get-live:getMissSync` | `3861593.415 ops/s` | `3730178.243 ops/s` | `-3.40%` | `warning` |
| `segment-index-get-multisegment-hot:getHitSync` | `7313.802 ops/s` | `7010.976 ops/s` | `-4.14%` | `warning` |
| `segment-index-get-multisegment-hot:getMissSync` | `3725333.768 ops/s` | `3621719.259 ops/s` | `-2.78%` | `neutral` |
| `segment-index-get-persisted:getHitSync` | `1918913.310 ops/s` | `1979811.795 ops/s` | `+3.17%` | `better` |
| `segment-index-get-persisted:getMissSync` | `3550616.668 ops/s` | `3685078.343 ops/s` | `+3.79%` | `better` |
| `segment-index-hot-route-put:putHotRoute` | `1960275.766 ops/s` | `1672247.886 ops/s` | `-14.69%` | `worse` |
| `segment-index-hot-route-put:putThenGetHotRoute` | `961383.161 ops/s` | `981315.651 ops/s` | `+2.07%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed` | `268741.747 ops/s` | `298104.160 ops/s` | `+10.93%` | `better` |
| `segment-index-mixed-drain:partitionedIngestMixed:getWorkload` | `120531.922 ops/s` | `115935.633 ops/s` | `-3.81%` | `warning` |
| `segment-index-mixed-drain:partitionedIngestMixed:putWorkload` | `148209.826 ops/s` | `182168.527 ops/s` | `+22.91%` | `better` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed` | `41594.156 ops/s` | `41355.880 ops/s` | `-0.57%` | `neutral` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:getWorkload` | `36291.665 ops/s` | `36099.481 ops/s` | `-0.53%` | `neutral` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:putWorkload` | `5302.492 ops/s` | `5256.400 ops/s` | `-0.87%` | `neutral` |
