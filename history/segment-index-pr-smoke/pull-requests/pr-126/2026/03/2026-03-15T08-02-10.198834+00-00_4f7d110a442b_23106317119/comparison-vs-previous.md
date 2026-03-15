# Benchmark Comparison

- Profile: `segment-index-pr-smoke`
- Baseline SHA: `52b04525c10e1f18faddd3a3d7f018427f1a5b53`
- Candidate SHA: `4f7d110a442b4eb348c02c44e1237e2b72c2114b`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `segment-index-get-overlay:getHitAsyncJoin` | `160151.872 ops/s` | `161936.215 ops/s` | `+1.11%` | `neutral` |
| `segment-index-get-overlay:getHitSync` | `4729456.975 ops/s` | `5299774.176 ops/s` | `+12.06%` | `better` |
| `segment-index-get-persisted:getHitAsyncJoin` | `54557.081 ops/s` | `54921.168 ops/s` | `+0.67%` | `neutral` |
| `segment-index-get-persisted:getHitSync` | `103784.125 ops/s` | `98794.286 ops/s` | `-4.81%` | `warning` |
| `segment-index-mixed-drain:partitionedIngestMixed` | `448575.461 ops/s` | `403516.137 ops/s` | `-10.04%` | `worse` |
| `segment-index-mixed-drain:partitionedIngestMixed:getWorkload` | `442786.229 ops/s` | `397728.001 ops/s` | `-10.18%` | `worse` |
| `segment-index-mixed-drain:partitionedIngestMixed:putWorkload` | `5789.232 ops/s` | `5788.137 ops/s` | `-0.02%` | `neutral` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed` | `-` | `175661.847 ops/s` | `-` | `new` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:getWorkload` | `-` | `173269.856 ops/s` | `-` | `new` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:putWorkload` | `-` | `2391.990 ops/s` | `-` | `new` |
