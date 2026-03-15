# Benchmark Comparison

- Profile: `segment-index-pr-smoke`
- Baseline SHA: `52b04525c10e1f18faddd3a3d7f018427f1a5b53`
- Candidate SHA: `7c7946623fd11813208dba6ce2fe93c02a33bc04`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `segment-index-get-overlay:getHitAsyncJoin` | `160151.872 ops/s` | `155485.581 ops/s` | `-2.91%` | `neutral` |
| `segment-index-get-overlay:getHitSync` | `4729456.975 ops/s` | `5318857.142 ops/s` | `+12.46%` | `better` |
| `segment-index-get-persisted:getHitAsyncJoin` | `54557.081 ops/s` | `55108.652 ops/s` | `+1.01%` | `neutral` |
| `segment-index-get-persisted:getHitSync` | `103784.125 ops/s` | `110246.695 ops/s` | `+6.23%` | `better` |
| `segment-index-mixed-drain:partitionedIngestMixed` | `448575.461 ops/s` | `447632.450 ops/s` | `-0.21%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed:getWorkload` | `442786.229 ops/s` | `441618.719 ops/s` | `-0.26%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed:putWorkload` | `5789.232 ops/s` | `6013.732 ops/s` | `+3.88%` | `better` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed` | `-` | `219160.564 ops/s` | `-` | `new` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:getWorkload` | `-` | `216774.669 ops/s` | `-` | `new` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:putWorkload` | `-` | `2385.895 ops/s` | `-` | `new` |
