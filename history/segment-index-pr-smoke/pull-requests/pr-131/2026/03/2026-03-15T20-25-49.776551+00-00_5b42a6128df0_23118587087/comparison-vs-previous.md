# Benchmark Comparison

- Profile: `segment-index-pr-smoke`
- Baseline SHA: `52b04525c10e1f18faddd3a3d7f018427f1a5b53`
- Candidate SHA: `5b42a6128df045250d823f7c6229545ae3fa5c06`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `segment-index-get-overlay:getHitAsyncJoin` | `160151.872 ops/s` | `175075.762 ops/s` | `+9.32%` | `better` |
| `segment-index-get-overlay:getHitSync` | `4729456.975 ops/s` | `5329090.153 ops/s` | `+12.68%` | `better` |
| `segment-index-get-persisted:getHitAsyncJoin` | `54557.081 ops/s` | `60652.398 ops/s` | `+11.17%` | `better` |
| `segment-index-get-persisted:getHitSync` | `103784.125 ops/s` | `103612.084 ops/s` | `-0.17%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed` | `448575.461 ops/s` | `461546.327 ops/s` | `+2.89%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed:getWorkload` | `442786.229 ops/s` | `455683.742 ops/s` | `+2.91%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed:putWorkload` | `5789.232 ops/s` | `5862.585 ops/s` | `+1.27%` | `neutral` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed` | `-` | `196624.362 ops/s` | `-` | `new` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:getWorkload` | `-` | `194009.891 ops/s` | `-` | `new` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:putWorkload` | `-` | `2614.471 ops/s` | `-` | `new` |
