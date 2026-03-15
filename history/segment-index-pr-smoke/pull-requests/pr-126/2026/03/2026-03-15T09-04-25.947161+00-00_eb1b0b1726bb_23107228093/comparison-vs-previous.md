# Benchmark Comparison

- Profile: `segment-index-pr-smoke`
- Baseline SHA: `52b04525c10e1f18faddd3a3d7f018427f1a5b53`
- Candidate SHA: `eb1b0b1726bbe84c52ecd7d81f0e0b3792984888`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `segment-index-get-overlay:getHitAsyncJoin` | `160151.872 ops/s` | `168813.573 ops/s` | `+5.41%` | `better` |
| `segment-index-get-overlay:getHitSync` | `4729456.975 ops/s` | `5363922.518 ops/s` | `+13.42%` | `better` |
| `segment-index-get-persisted:getHitAsyncJoin` | `54557.081 ops/s` | `60561.036 ops/s` | `+11.00%` | `better` |
| `segment-index-get-persisted:getHitSync` | `103784.125 ops/s` | `114801.869 ops/s` | `+10.62%` | `better` |
| `segment-index-mixed-drain:partitionedIngestMixed` | `448575.461 ops/s` | `446059.987 ops/s` | `-0.56%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed:getWorkload` | `442786.229 ops/s` | `439981.909 ops/s` | `-0.63%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed:putWorkload` | `5789.232 ops/s` | `6078.078 ops/s` | `+4.99%` | `better` |
