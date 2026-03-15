# Benchmark Comparison

- Profile: `segment-index-pr-smoke`
- Baseline SHA: `52b04525c10e1f18faddd3a3d7f018427f1a5b53`
- Candidate SHA: `1480a2b54912acaa122507fc4deb9ab0195a7145`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `segment-index-get-overlay:getHitAsyncJoin` | `160151.872 ops/s` | `155218.181 ops/s` | `-3.08%` | `warning` |
| `segment-index-get-overlay:getHitSync` | `4729456.975 ops/s` | `5308245.274 ops/s` | `+12.24%` | `better` |
| `segment-index-get-persisted:getHitAsyncJoin` | `54557.081 ops/s` | `56931.625 ops/s` | `+4.35%` | `better` |
| `segment-index-get-persisted:getHitSync` | `103784.125 ops/s` | `112683.332 ops/s` | `+8.57%` | `better` |
| `segment-index-mixed-drain:partitionedIngestMixed` | `448575.461 ops/s` | `459751.217 ops/s` | `+2.49%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed:getWorkload` | `442786.229 ops/s` | `453793.143 ops/s` | `+2.49%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed:putWorkload` | `5789.232 ops/s` | `5958.074 ops/s` | `+2.92%` | `neutral` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed` | `-` | `215659.714 ops/s` | `-` | `new` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:getWorkload` | `-` | `212971.015 ops/s` | `-` | `new` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:putWorkload` | `-` | `2688.698 ops/s` | `-` | `new` |
