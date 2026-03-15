# Benchmark Comparison

- Profile: `segment-index-pr-smoke`
- Baseline SHA: `52b04525c10e1f18faddd3a3d7f018427f1a5b53`
- Candidate SHA: `288b76a9d3c0992bb0e4afa66fdb5416d245dc73`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `segment-index-get-overlay:getHitAsyncJoin` | `160151.872 ops/s` | `160616.254 ops/s` | `+0.29%` | `neutral` |
| `segment-index-get-overlay:getHitSync` | `4729456.975 ops/s` | `5288161.162 ops/s` | `+11.81%` | `better` |
| `segment-index-get-persisted:getHitAsyncJoin` | `54557.081 ops/s` | `56071.716 ops/s` | `+2.78%` | `neutral` |
| `segment-index-get-persisted:getHitSync` | `103784.125 ops/s` | `101655.101 ops/s` | `-2.05%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed` | `448575.461 ops/s` | `458028.319 ops/s` | `+2.11%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed:getWorkload` | `442786.229 ops/s` | `452000.321 ops/s` | `+2.08%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed:putWorkload` | `5789.232 ops/s` | `6027.998 ops/s` | `+4.12%` | `better` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed` | `-` | `208607.422 ops/s` | `-` | `new` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:getWorkload` | `-` | `206120.028 ops/s` | `-` | `new` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:putWorkload` | `-` | `2487.394 ops/s` | `-` | `new` |
