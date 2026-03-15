# Benchmark Comparison

- Profile: `segment-index-pr-smoke`
- Baseline SHA: `52b04525c10e1f18faddd3a3d7f018427f1a5b53`
- Candidate SHA: `f1d0f0b947014bafe88457887e894f85d63728d2`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `segment-index-get-overlay:getHitAsyncJoin` | `160151.872 ops/s` | `173502.328 ops/s` | `+8.34%` | `better` |
| `segment-index-get-overlay:getHitSync` | `4729456.975 ops/s` | `5127719.494 ops/s` | `+8.42%` | `better` |
| `segment-index-get-persisted:getHitAsyncJoin` | `54557.081 ops/s` | `53365.232 ops/s` | `-2.18%` | `neutral` |
| `segment-index-get-persisted:getHitSync` | `103784.125 ops/s` | `105324.197 ops/s` | `+1.48%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed` | `448575.461 ops/s` | `474765.333 ops/s` | `+5.84%` | `better` |
| `segment-index-mixed-drain:partitionedIngestMixed:getWorkload` | `442786.229 ops/s` | `468855.754 ops/s` | `+5.89%` | `better` |
| `segment-index-mixed-drain:partitionedIngestMixed:putWorkload` | `5789.232 ops/s` | `5909.578 ops/s` | `+2.08%` | `neutral` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed` | `-` | `200680.669 ops/s` | `-` | `new` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:getWorkload` | `-` | `185445.220 ops/s` | `-` | `new` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:putWorkload` | `-` | `15235.449 ops/s` | `-` | `new` |
