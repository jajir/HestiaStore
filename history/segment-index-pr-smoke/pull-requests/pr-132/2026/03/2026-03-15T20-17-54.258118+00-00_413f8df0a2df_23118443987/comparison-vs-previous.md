# Benchmark Comparison

- Profile: `segment-index-pr-smoke`
- Baseline SHA: `52b04525c10e1f18faddd3a3d7f018427f1a5b53`
- Candidate SHA: `413f8df0a2df2bdb84efa6fef4926969d856f8ca`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `segment-index-get-overlay:getHitAsyncJoin` | `160151.872 ops/s` | `159007.975 ops/s` | `-0.71%` | `neutral` |
| `segment-index-get-overlay:getHitSync` | `4729456.975 ops/s` | `5180718.376 ops/s` | `+9.54%` | `better` |
| `segment-index-get-persisted:getHitAsyncJoin` | `54557.081 ops/s` | `53216.397 ops/s` | `-2.46%` | `neutral` |
| `segment-index-get-persisted:getHitSync` | `103784.125 ops/s` | `107078.227 ops/s` | `+3.17%` | `better` |
| `segment-index-mixed-drain:partitionedIngestMixed` | `448575.461 ops/s` | `503136.474 ops/s` | `+12.16%` | `better` |
| `segment-index-mixed-drain:partitionedIngestMixed:getWorkload` | `442786.229 ops/s` | `497321.601 ops/s` | `+12.32%` | `better` |
| `segment-index-mixed-drain:partitionedIngestMixed:putWorkload` | `5789.232 ops/s` | `5814.873 ops/s` | `+0.44%` | `neutral` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed` | `-` | `217285.111 ops/s` | `-` | `new` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:getWorkload` | `-` | `214640.660 ops/s` | `-` | `new` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:putWorkload` | `-` | `2644.451 ops/s` | `-` | `new` |
