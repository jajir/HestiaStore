# Benchmark Comparison

- Profile: `segment-index-pr-smoke`
- Baseline SHA: `52b04525c10e1f18faddd3a3d7f018427f1a5b53`
- Candidate SHA: `e38cf4a02cc53dabef960e976f21a7abe12d024c`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `segment-index-get-overlay:getHitAsyncJoin` | `160151.872 ops/s` | `162531.707 ops/s` | `+1.49%` | `neutral` |
| `segment-index-get-overlay:getHitSync` | `4729456.975 ops/s` | `5034699.911 ops/s` | `+6.45%` | `better` |
| `segment-index-get-persisted:getHitAsyncJoin` | `54557.081 ops/s` | `59573.707 ops/s` | `+9.20%` | `better` |
| `segment-index-get-persisted:getHitSync` | `103784.125 ops/s` | `95149.802 ops/s` | `-8.32%` | `worse` |
| `segment-index-mixed-drain:partitionedIngestMixed` | `448575.461 ops/s` | `438725.029 ops/s` | `-2.20%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed:getWorkload` | `442786.229 ops/s` | `432883.378 ops/s` | `-2.24%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed:putWorkload` | `5789.232 ops/s` | `5841.651 ops/s` | `+0.91%` | `neutral` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed` | `-` | `205758.276 ops/s` | `-` | `new` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:getWorkload` | `-` | `203051.976 ops/s` | `-` | `new` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:putWorkload` | `-` | `2706.300 ops/s` | `-` | `new` |
