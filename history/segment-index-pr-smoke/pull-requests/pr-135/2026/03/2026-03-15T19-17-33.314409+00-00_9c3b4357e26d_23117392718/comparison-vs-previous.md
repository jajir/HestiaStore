# Benchmark Comparison

- Profile: `segment-index-pr-smoke`
- Baseline SHA: `52b04525c10e1f18faddd3a3d7f018427f1a5b53`
- Candidate SHA: `9c3b4357e26d9470f47a1e6f65ed74e19dce00e4`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `segment-index-get-overlay:getHitAsyncJoin` | `160151.872 ops/s` | `159609.240 ops/s` | `-0.34%` | `neutral` |
| `segment-index-get-overlay:getHitSync` | `4729456.975 ops/s` | `5360358.635 ops/s` | `+13.34%` | `better` |
| `segment-index-get-persisted:getHitAsyncJoin` | `54557.081 ops/s` | `56416.321 ops/s` | `+3.41%` | `better` |
| `segment-index-get-persisted:getHitSync` | `103784.125 ops/s` | `111834.340 ops/s` | `+7.76%` | `better` |
| `segment-index-mixed-drain:partitionedIngestMixed` | `448575.461 ops/s` | `430232.898 ops/s` | `-4.09%` | `warning` |
| `segment-index-mixed-drain:partitionedIngestMixed:getWorkload` | `442786.229 ops/s` | `424298.967 ops/s` | `-4.18%` | `warning` |
| `segment-index-mixed-drain:partitionedIngestMixed:putWorkload` | `5789.232 ops/s` | `5933.931 ops/s` | `+2.50%` | `neutral` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed` | `-` | `192821.186 ops/s` | `-` | `new` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:getWorkload` | `-` | `190150.152 ops/s` | `-` | `new` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:putWorkload` | `-` | `2671.034 ops/s` | `-` | `new` |
