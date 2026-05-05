# Benchmark Comparison

- Profile: `segment-index-pr-smoke`
- Baseline SHA: `1bf3c96c4778c8cca6edc361e68f8c45fc3d9083`
- Candidate SHA: `c5d81ba5c428bf7abdbbedd02c0370f2ec9d3e47`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `segment-index-get-live:getHitSync` | `2369528.705 ops/s` | `2305602.132 ops/s` | `-2.70%` | `neutral` |
| `segment-index-get-live:getMissSync` | `3730179.464 ops/s` | `3792916.099 ops/s` | `+1.68%` | `neutral` |
| `segment-index-get-multisegment-hot:getHitSync` | `7132.769 ops/s` | `7432.595 ops/s` | `+4.20%` | `better` |
| `segment-index-get-multisegment-hot:getMissSync` | `3726597.753 ops/s` | `3630349.450 ops/s` | `-2.58%` | `neutral` |
| `segment-index-get-persisted:getHitSync` | `119024.024 ops/s` | `121160.548 ops/s` | `+1.80%` | `neutral` |
| `segment-index-get-persisted:getMissSync` | `4003546.285 ops/s` | `3969124.235 ops/s` | `-0.86%` | `neutral` |
| `segment-index-hot-route-put:putHotRoute` | `1920380.902 ops/s` | `2108325.941 ops/s` | `+9.79%` | `better` |
| `segment-index-hot-route-put:putThenGetHotRoute` | `1087420.729 ops/s` | `1058210.506 ops/s` | `-2.69%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed` | `289156.511 ops/s` | `304602.820 ops/s` | `+5.34%` | `better` |
| `segment-index-mixed-drain:partitionedIngestMixed:getWorkload` | `124584.710 ops/s` | `135905.063 ops/s` | `+9.09%` | `better` |
| `segment-index-mixed-drain:partitionedIngestMixed:putWorkload` | `164571.801 ops/s` | `168697.757 ops/s` | `+2.51%` | `neutral` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed` | `41398.166 ops/s` | `43401.260 ops/s` | `+4.84%` | `better` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:getWorkload` | `36190.796 ops/s` | `38119.009 ops/s` | `+5.33%` | `better` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:putWorkload` | `5207.370 ops/s` | `5282.252 ops/s` | `+1.44%` | `neutral` |
| `segment-index-persisted-mutation:deleteSync` | `2547.386 ops/s` | `2644.262 ops/s` | `+3.80%` | `better` |
| `segment-index-persisted-mutation:putSync` | `448.434 ops/s` | `443.682 ops/s` | `-1.06%` | `neutral` |
