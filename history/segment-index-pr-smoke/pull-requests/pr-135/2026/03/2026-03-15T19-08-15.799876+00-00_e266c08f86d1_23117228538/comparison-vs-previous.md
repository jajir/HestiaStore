# Benchmark Comparison

- Profile: `segment-index-pr-smoke`
- Baseline SHA: `52b04525c10e1f18faddd3a3d7f018427f1a5b53`
- Candidate SHA: `e266c08f86d115240b6c7ea7e383dc19d446387c`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `segment-index-get-overlay:getHitAsyncJoin` | `160151.872 ops/s` | `166159.030 ops/s` | `+3.75%` | `better` |
| `segment-index-get-overlay:getHitSync` | `4729456.975 ops/s` | `5398789.814 ops/s` | `+14.15%` | `better` |
| `segment-index-get-persisted:getHitAsyncJoin` | `54557.081 ops/s` | `56004.948 ops/s` | `+2.65%` | `neutral` |
| `segment-index-get-persisted:getHitSync` | `103784.125 ops/s` | `105908.042 ops/s` | `+2.05%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed` | `448575.461 ops/s` | `418680.032 ops/s` | `-6.66%` | `warning` |
| `segment-index-mixed-drain:partitionedIngestMixed:getWorkload` | `442786.229 ops/s` | `412754.766 ops/s` | `-6.78%` | `warning` |
| `segment-index-mixed-drain:partitionedIngestMixed:putWorkload` | `5789.232 ops/s` | `5925.266 ops/s` | `+2.35%` | `neutral` |
