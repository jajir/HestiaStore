# Benchmark Comparison

- Profile: `segment-index-pr-smoke`
- Baseline SHA: `52b04525c10e1f18faddd3a3d7f018427f1a5b53`
- Candidate SHA: `5643f780c41db62624c2fd58c5c3becea8fcb1de`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `segment-index-get-overlay:getHitAsyncJoin` | `160151.872 ops/s` | `157663.870 ops/s` | `-1.55%` | `neutral` |
| `segment-index-get-overlay:getHitSync` | `4729456.975 ops/s` | `5410646.340 ops/s` | `+14.40%` | `better` |
| `segment-index-get-persisted:getHitAsyncJoin` | `54557.081 ops/s` | `55878.621 ops/s` | `+2.42%` | `neutral` |
| `segment-index-get-persisted:getHitSync` | `103784.125 ops/s` | `103766.648 ops/s` | `-0.02%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed` | `448575.461 ops/s` | `450930.045 ops/s` | `+0.52%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed:getWorkload` | `442786.229 ops/s` | `444995.479 ops/s` | `+0.50%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed:putWorkload` | `5789.232 ops/s` | `5934.565 ops/s` | `+2.51%` | `neutral` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed` | `-` | `189444.113 ops/s` | `-` | `new` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:getWorkload` | `-` | `186658.112 ops/s` | `-` | `new` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:putWorkload` | `-` | `2786.001 ops/s` | `-` | `new` |
