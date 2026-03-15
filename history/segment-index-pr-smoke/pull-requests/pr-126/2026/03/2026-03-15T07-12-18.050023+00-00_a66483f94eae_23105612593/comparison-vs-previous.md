# Benchmark Comparison

- Profile: `segment-index-pr-smoke`
- Baseline SHA: `52b04525c10e1f18faddd3a3d7f018427f1a5b53`
- Candidate SHA: `a66483f94eaef3cc7aa7df202797002013469069`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `segment-index-get-overlay:getHitAsyncJoin` | `160151.872 ops/s` | `245509.387 ops/s` | `+53.30%` | `better` |
| `segment-index-get-overlay:getHitSync` | `4729456.975 ops/s` | `3736671.727 ops/s` | `-20.99%` | `worse` |
| `segment-index-get-persisted:getHitAsyncJoin` | `54557.081 ops/s` | `73775.906 ops/s` | `+35.23%` | `better` |
| `segment-index-get-persisted:getHitSync` | `103784.125 ops/s` | `111045.035 ops/s` | `+7.00%` | `better` |
| `segment-index-mixed-drain:partitionedIngestMixed` | `448575.461 ops/s` | `429408.210 ops/s` | `-4.27%` | `warning` |
| `segment-index-mixed-drain:partitionedIngestMixed:getWorkload` | `442786.229 ops/s` | `423630.331 ops/s` | `-4.33%` | `warning` |
| `segment-index-mixed-drain:partitionedIngestMixed:putWorkload` | `5789.232 ops/s` | `5777.879 ops/s` | `-0.20%` | `neutral` |
