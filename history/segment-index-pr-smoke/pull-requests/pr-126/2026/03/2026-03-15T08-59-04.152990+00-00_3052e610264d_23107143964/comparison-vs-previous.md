# Benchmark Comparison

- Profile: `segment-index-pr-smoke`
- Baseline SHA: `52b04525c10e1f18faddd3a3d7f018427f1a5b53`
- Candidate SHA: `3052e610264df8f250b3cac81f0c4f395ed13de3`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `segment-index-get-overlay:getHitAsyncJoin` | `160151.872 ops/s` | `164645.519 ops/s` | `+2.81%` | `neutral` |
| `segment-index-get-overlay:getHitSync` | `4729456.975 ops/s` | `5343095.488 ops/s` | `+12.97%` | `better` |
| `segment-index-get-persisted:getHitAsyncJoin` | `54557.081 ops/s` | `57067.185 ops/s` | `+4.60%` | `better` |
| `segment-index-get-persisted:getHitSync` | `103784.125 ops/s` | `109076.511 ops/s` | `+5.10%` | `better` |
| `segment-index-mixed-drain:partitionedIngestMixed` | `448575.461 ops/s` | `475086.017 ops/s` | `+5.91%` | `better` |
| `segment-index-mixed-drain:partitionedIngestMixed:getWorkload` | `442786.229 ops/s` | `469283.269 ops/s` | `+5.98%` | `better` |
| `segment-index-mixed-drain:partitionedIngestMixed:putWorkload` | `5789.232 ops/s` | `5802.747 ops/s` | `+0.23%` | `neutral` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed` | `-` | `205756.132 ops/s` | `-` | `new` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:getWorkload` | `-` | `202967.115 ops/s` | `-` | `new` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:putWorkload` | `-` | `2789.017 ops/s` | `-` | `new` |
