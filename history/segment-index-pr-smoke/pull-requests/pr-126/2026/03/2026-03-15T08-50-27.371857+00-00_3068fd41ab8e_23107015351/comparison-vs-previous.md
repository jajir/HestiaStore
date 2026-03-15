# Benchmark Comparison

- Profile: `segment-index-pr-smoke`
- Baseline SHA: `52b04525c10e1f18faddd3a3d7f018427f1a5b53`
- Candidate SHA: `3068fd41ab8e30395ee7f71a6c08842c3c8485bb`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `segment-index-get-overlay:getHitAsyncJoin` | `160151.872 ops/s` | `163148.372 ops/s` | `+1.87%` | `neutral` |
| `segment-index-get-overlay:getHitSync` | `4729456.975 ops/s` | `5233218.292 ops/s` | `+10.65%` | `better` |
| `segment-index-get-persisted:getHitAsyncJoin` | `54557.081 ops/s` | `56306.631 ops/s` | `+3.21%` | `better` |
| `segment-index-get-persisted:getHitSync` | `103784.125 ops/s` | `103411.055 ops/s` | `-0.36%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed` | `448575.461 ops/s` | `492296.873 ops/s` | `+9.75%` | `better` |
| `segment-index-mixed-drain:partitionedIngestMixed:getWorkload` | `442786.229 ops/s` | `486285.955 ops/s` | `+9.82%` | `better` |
| `segment-index-mixed-drain:partitionedIngestMixed:putWorkload` | `5789.232 ops/s` | `6010.918 ops/s` | `+3.83%` | `better` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed` | `-` | `198280.422 ops/s` | `-` | `new` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:getWorkload` | `-` | `195546.621 ops/s` | `-` | `new` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:putWorkload` | `-` | `2733.801 ops/s` | `-` | `new` |
