# Benchmark Comparison

- Profile: `segment-index-pr-smoke`
- Baseline SHA: `52b04525c10e1f18faddd3a3d7f018427f1a5b53`
- Candidate SHA: `9424ef90d214b8fe3d11347be99af1bd0cd5c08b`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `segment-index-get-overlay:getHitAsyncJoin` | `160151.872 ops/s` | `165835.858 ops/s` | `+3.55%` | `better` |
| `segment-index-get-overlay:getHitSync` | `4729456.975 ops/s` | `5563418.653 ops/s` | `+17.63%` | `better` |
| `segment-index-get-persisted:getHitAsyncJoin` | `54557.081 ops/s` | `56078.955 ops/s` | `+2.79%` | `neutral` |
| `segment-index-get-persisted:getHitSync` | `103784.125 ops/s` | `107562.936 ops/s` | `+3.64%` | `better` |
| `segment-index-mixed-drain:partitionedIngestMixed` | `448575.461 ops/s` | `455020.385 ops/s` | `+1.44%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed:getWorkload` | `442786.229 ops/s` | `449009.113 ops/s` | `+1.41%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed:putWorkload` | `5789.232 ops/s` | `6011.272 ops/s` | `+3.84%` | `better` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed` | `-` | `205631.337 ops/s` | `-` | `new` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:getWorkload` | `-` | `202911.380 ops/s` | `-` | `new` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:putWorkload` | `-` | `2719.957 ops/s` | `-` | `new` |
