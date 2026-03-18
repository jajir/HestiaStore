# Benchmark Comparison

- Profile: `segment-index-pr-smoke`
- Baseline SHA: `52b04525c10e1f18faddd3a3d7f018427f1a5b53`
- Candidate SHA: `00a283188c1326e13a357bff82f4ad8e67f9306e`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `segment-index-get-multisegment-hot:getHitAsyncJoin` | `-` | `101.961 ops/s` | `-` | `new` |
| `segment-index-get-multisegment-hot:getHitSync` | `-` | `97.571 ops/s` | `-` | `new` |
| `segment-index-get-multisegment-hot:getMissAsyncJoin` | `-` | `165653.173 ops/s` | `-` | `new` |
| `segment-index-get-multisegment-hot:getMissSync` | `-` | `6651985.832 ops/s` | `-` | `new` |
| `segment-index-get-overlay:getHitAsyncJoin` | `160151.872 ops/s` | `162432.906 ops/s` | `+1.42%` | `neutral` |
| `segment-index-get-overlay:getHitSync` | `4729456.975 ops/s` | `5305250.810 ops/s` | `+12.17%` | `better` |
| `segment-index-get-overlay:getMissAsyncJoin` | `-` | `166188.697 ops/s` | `-` | `new` |
| `segment-index-get-overlay:getMissSync` | `-` | `6606576.660 ops/s` | `-` | `new` |
| `segment-index-get-persisted:getHitAsyncJoin` | `54557.081 ops/s` | `57411.573 ops/s` | `+5.23%` | `better` |
| `segment-index-get-persisted:getHitSync` | `103784.125 ops/s` | `108596.569 ops/s` | `+4.64%` | `better` |
| `segment-index-get-persisted:getMissAsyncJoin` | `-` | `156530.747 ops/s` | `-` | `new` |
| `segment-index-get-persisted:getMissSync` | `-` | `6687317.313 ops/s` | `-` | `new` |
| `segment-index-mixed-drain:partitionedIngestMixed` | `448575.461 ops/s` | `409802.020 ops/s` | `-8.64%` | `worse` |
| `segment-index-mixed-drain:partitionedIngestMixed:getWorkload` | `442786.229 ops/s` | `403986.822 ops/s` | `-8.76%` | `worse` |
| `segment-index-mixed-drain:partitionedIngestMixed:putWorkload` | `5789.232 ops/s` | `5815.199 ops/s` | `+0.45%` | `neutral` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed` | `-` | `199267.390 ops/s` | `-` | `new` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:getWorkload` | `-` | `196554.362 ops/s` | `-` | `new` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:putWorkload` | `-` | `2713.028 ops/s` | `-` | `new` |
