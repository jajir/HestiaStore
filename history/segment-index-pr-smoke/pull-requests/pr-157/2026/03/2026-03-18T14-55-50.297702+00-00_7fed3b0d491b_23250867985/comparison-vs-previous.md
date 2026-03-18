# Benchmark Comparison

- Profile: `segment-index-pr-smoke`
- Baseline SHA: `52b04525c10e1f18faddd3a3d7f018427f1a5b53`
- Candidate SHA: `7fed3b0d491b9bf9c5393d6aee31bfa391d8f5c3`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `segment-index-get-multisegment-hot:getHitAsyncJoin` | `-` | `95.443 ops/s` | `-` | `new` |
| `segment-index-get-multisegment-hot:getHitSync` | `-` | `102.076 ops/s` | `-` | `new` |
| `segment-index-get-multisegment-hot:getMissAsyncJoin` | `-` | `164750.367 ops/s` | `-` | `new` |
| `segment-index-get-multisegment-hot:getMissSync` | `-` | `6098711.945 ops/s` | `-` | `new` |
| `segment-index-get-overlay:getHitAsyncJoin` | `160151.872 ops/s` | `152231.307 ops/s` | `-4.95%` | `warning` |
| `segment-index-get-overlay:getHitSync` | `4729456.975 ops/s` | `5508956.207 ops/s` | `+16.48%` | `better` |
| `segment-index-get-overlay:getMissAsyncJoin` | `-` | `166146.083 ops/s` | `-` | `new` |
| `segment-index-get-overlay:getMissSync` | `-` | `6977410.078 ops/s` | `-` | `new` |
| `segment-index-get-persisted:getHitAsyncJoin` | `54557.081 ops/s` | `58247.701 ops/s` | `+6.76%` | `better` |
| `segment-index-get-persisted:getHitSync` | `103784.125 ops/s` | `108845.132 ops/s` | `+4.88%` | `better` |
| `segment-index-get-persisted:getMissAsyncJoin` | `-` | `163515.398 ops/s` | `-` | `new` |
| `segment-index-get-persisted:getMissSync` | `-` | `7064846.003 ops/s` | `-` | `new` |
| `segment-index-mixed-drain:partitionedIngestMixed` | `448575.461 ops/s` | `439244.254 ops/s` | `-2.08%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed:getWorkload` | `442786.229 ops/s` | `433201.643 ops/s` | `-2.16%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed:putWorkload` | `5789.232 ops/s` | `6042.611 ops/s` | `+4.38%` | `better` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed` | `-` | `221920.527 ops/s` | `-` | `new` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:getWorkload` | `-` | `219371.992 ops/s` | `-` | `new` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:putWorkload` | `-` | `2548.535 ops/s` | `-` | `new` |
