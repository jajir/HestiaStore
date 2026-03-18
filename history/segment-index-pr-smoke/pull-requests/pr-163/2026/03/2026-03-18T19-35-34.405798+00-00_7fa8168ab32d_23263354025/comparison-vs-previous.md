# Benchmark Comparison

- Profile: `segment-index-pr-smoke`
- Baseline SHA: `52b04525c10e1f18faddd3a3d7f018427f1a5b53`
- Candidate SHA: `7fa8168ab32d2db0a0656bf8b99f27c00de8126c`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `segment-index-get-multisegment-hot:getHitAsyncJoin` | `-` | `101.208 ops/s` | `-` | `new` |
| `segment-index-get-multisegment-hot:getHitSync` | `-` | `98.549 ops/s` | `-` | `new` |
| `segment-index-get-multisegment-hot:getMissAsyncJoin` | `-` | `170490.261 ops/s` | `-` | `new` |
| `segment-index-get-multisegment-hot:getMissSync` | `-` | `6364408.492 ops/s` | `-` | `new` |
| `segment-index-get-overlay:getHitAsyncJoin` | `160151.872 ops/s` | `167807.813 ops/s` | `+4.78%` | `better` |
| `segment-index-get-overlay:getHitSync` | `4729456.975 ops/s` | `5213885.770 ops/s` | `+10.24%` | `better` |
| `segment-index-get-overlay:getMissAsyncJoin` | `-` | `170355.261 ops/s` | `-` | `new` |
| `segment-index-get-overlay:getMissSync` | `-` | `7556891.581 ops/s` | `-` | `new` |
| `segment-index-get-persisted:getHitAsyncJoin` | `54557.081 ops/s` | `57780.340 ops/s` | `+5.91%` | `better` |
| `segment-index-get-persisted:getHitSync` | `103784.125 ops/s` | `109537.515 ops/s` | `+5.54%` | `better` |
| `segment-index-get-persisted:getMissAsyncJoin` | `-` | `172689.165 ops/s` | `-` | `new` |
| `segment-index-get-persisted:getMissSync` | `-` | `6890065.669 ops/s` | `-` | `new` |
| `segment-index-mixed-drain:partitionedIngestMixed` | `448575.461 ops/s` | `463486.177 ops/s` | `+3.32%` | `better` |
| `segment-index-mixed-drain:partitionedIngestMixed:getWorkload` | `442786.229 ops/s` | `457820.557 ops/s` | `+3.40%` | `better` |
| `segment-index-mixed-drain:partitionedIngestMixed:putWorkload` | `5789.232 ops/s` | `5665.620 ops/s` | `-2.14%` | `neutral` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed` | `-` | `196303.742 ops/s` | `-` | `new` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:getWorkload` | `-` | `193693.014 ops/s` | `-` | `new` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:putWorkload` | `-` | `2610.728 ops/s` | `-` | `new` |
