# Benchmark Comparison

- Profile: `segment-index-pr-smoke`
- Baseline SHA: `52b04525c10e1f18faddd3a3d7f018427f1a5b53`
- Candidate SHA: `47391b04890ddd73b3d87ba73f5fa30cd2e86189`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `segment-index-get-overlay:getHitAsyncJoin` | `160151.872 ops/s` | `174538.481 ops/s` | `+8.98%` | `better` |
| `segment-index-get-overlay:getHitSync` | `4729456.975 ops/s` | `5071647.597 ops/s` | `+7.24%` | `better` |
| `segment-index-get-persisted:getHitAsyncJoin` | `54557.081 ops/s` | `59559.974 ops/s` | `+9.17%` | `better` |
| `segment-index-get-persisted:getHitSync` | `103784.125 ops/s` | `112220.000 ops/s` | `+8.13%` | `better` |
| `segment-index-mixed-drain:partitionedIngestMixed` | `448575.461 ops/s` | `449606.705 ops/s` | `+0.23%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed:getWorkload` | `442786.229 ops/s` | `443621.537 ops/s` | `+0.19%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed:putWorkload` | `5789.232 ops/s` | `5985.168 ops/s` | `+3.38%` | `better` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed` | `-` | `203490.600 ops/s` | `-` | `new` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:getWorkload` | `-` | `200806.836 ops/s` | `-` | `new` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:putWorkload` | `-` | `2683.764 ops/s` | `-` | `new` |
