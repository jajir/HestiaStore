# Benchmark Comparison

- Profile: `segment-index-pr-smoke`
- Baseline SHA: `52b04525c10e1f18faddd3a3d7f018427f1a5b53`
- Candidate SHA: `d1a64762610e2138053c39b70b086039957f7813`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `segment-index-get-overlay:getHitAsyncJoin` | `160151.872 ops/s` | `161536.707 ops/s` | `+0.86%` | `neutral` |
| `segment-index-get-overlay:getHitSync` | `4729456.975 ops/s` | `5309423.954 ops/s` | `+12.26%` | `better` |
| `segment-index-get-persisted:getHitAsyncJoin` | `54557.081 ops/s` | `54409.940 ops/s` | `-0.27%` | `neutral` |
| `segment-index-get-persisted:getHitSync` | `103784.125 ops/s` | `107949.998 ops/s` | `+4.01%` | `better` |
| `segment-index-mixed-drain:partitionedIngestMixed` | `448575.461 ops/s` | `442489.693 ops/s` | `-1.36%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed:getWorkload` | `442786.229 ops/s` | `436727.269 ops/s` | `-1.37%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed:putWorkload` | `5789.232 ops/s` | `5762.423 ops/s` | `-0.46%` | `neutral` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed` | `-` | `222120.128 ops/s` | `-` | `new` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:getWorkload` | `-` | `219554.387 ops/s` | `-` | `new` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:putWorkload` | `-` | `2565.741 ops/s` | `-` | `new` |
