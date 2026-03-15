# Benchmark Comparison

- Profile: `segment-index-pr-smoke`
- Baseline SHA: `52b04525c10e1f18faddd3a3d7f018427f1a5b53`
- Candidate SHA: `759d58e2366c320edc21a073c28753b35ac49edb`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `segment-index-get-overlay:getHitAsyncJoin` | `160151.872 ops/s` | `162439.473 ops/s` | `+1.43%` | `neutral` |
| `segment-index-get-overlay:getHitSync` | `4729456.975 ops/s` | `5435740.730 ops/s` | `+14.93%` | `better` |
| `segment-index-get-persisted:getHitAsyncJoin` | `54557.081 ops/s` | `57160.412 ops/s` | `+4.77%` | `better` |
| `segment-index-get-persisted:getHitSync` | `103784.125 ops/s` | `109346.361 ops/s` | `+5.36%` | `better` |
| `segment-index-mixed-drain:partitionedIngestMixed` | `448575.461 ops/s` | `464738.090 ops/s` | `+3.60%` | `better` |
| `segment-index-mixed-drain:partitionedIngestMixed:getWorkload` | `442786.229 ops/s` | `459046.760 ops/s` | `+3.67%` | `better` |
| `segment-index-mixed-drain:partitionedIngestMixed:putWorkload` | `5789.232 ops/s` | `5691.330 ops/s` | `-1.69%` | `neutral` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed` | `-` | `191017.820 ops/s` | `-` | `new` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:getWorkload` | `-` | `188289.570 ops/s` | `-` | `new` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:putWorkload` | `-` | `2728.249 ops/s` | `-` | `new` |
