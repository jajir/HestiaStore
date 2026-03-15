# Benchmark Comparison

- Profile: `segment-index-pr-smoke`
- Baseline SHA: `52b04525c10e1f18faddd3a3d7f018427f1a5b53`
- Candidate SHA: `fd838c3c77c9a449c84a04d1557874095f7aa8a0`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `segment-index-get-overlay:getHitAsyncJoin` | `160151.872 ops/s` | `164554.961 ops/s` | `+2.75%` | `neutral` |
| `segment-index-get-overlay:getHitSync` | `4729456.975 ops/s` | `5322377.817 ops/s` | `+12.54%` | `better` |
| `segment-index-get-persisted:getHitAsyncJoin` | `54557.081 ops/s` | `59119.102 ops/s` | `+8.36%` | `better` |
| `segment-index-get-persisted:getHitSync` | `103784.125 ops/s` | `111299.219 ops/s` | `+7.24%` | `better` |
| `segment-index-mixed-drain:partitionedIngestMixed` | `448575.461 ops/s` | `481630.835 ops/s` | `+7.37%` | `better` |
| `segment-index-mixed-drain:partitionedIngestMixed:getWorkload` | `442786.229 ops/s` | `475601.380 ops/s` | `+7.41%` | `better` |
| `segment-index-mixed-drain:partitionedIngestMixed:putWorkload` | `5789.232 ops/s` | `6029.455 ops/s` | `+4.15%` | `better` |
