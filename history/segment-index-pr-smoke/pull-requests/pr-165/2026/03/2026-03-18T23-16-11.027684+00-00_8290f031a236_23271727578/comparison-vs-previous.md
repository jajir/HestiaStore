# Benchmark Comparison

- Profile: `segment-index-pr-smoke`
- Baseline SHA: `52b04525c10e1f18faddd3a3d7f018427f1a5b53`
- Candidate SHA: `8290f031a236c087a812988b4b1876a752ff767e`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `segment-index-get-multisegment-hot:getHitAsyncJoin` | `-` | `90.193 ops/s` | `-` | `new` |
| `segment-index-get-multisegment-hot:getHitSync` | `-` | `88.347 ops/s` | `-` | `new` |
| `segment-index-get-multisegment-hot:getMissAsyncJoin` | `-` | `177432.207 ops/s` | `-` | `new` |
| `segment-index-get-multisegment-hot:getMissSync` | `-` | `4054061.987 ops/s` | `-` | `new` |
| `segment-index-get-overlay:getHitAsyncJoin` | `160151.872 ops/s` | `164901.618 ops/s` | `+2.97%` | `neutral` |
| `segment-index-get-overlay:getHitSync` | `4729456.975 ops/s` | `4220009.286 ops/s` | `-10.77%` | `worse` |
| `segment-index-get-overlay:getMissAsyncJoin` | `-` | `176678.476 ops/s` | `-` | `new` |
| `segment-index-get-overlay:getMissSync` | `-` | `4031289.336 ops/s` | `-` | `new` |
| `segment-index-get-persisted:getHitAsyncJoin` | `54557.081 ops/s` | `58016.799 ops/s` | `+6.34%` | `better` |
| `segment-index-get-persisted:getHitSync` | `103784.125 ops/s` | `112709.375 ops/s` | `+8.60%` | `better` |
| `segment-index-get-persisted:getMissAsyncJoin` | `-` | `174995.218 ops/s` | `-` | `new` |
| `segment-index-get-persisted:getMissSync` | `-` | `4106967.750 ops/s` | `-` | `new` |
| `segment-index-mixed-drain:partitionedIngestMixed` | `448575.461 ops/s` | `414860.345 ops/s` | `-7.52%` | `worse` |
| `segment-index-mixed-drain:partitionedIngestMixed:getWorkload` | `442786.229 ops/s` | `409657.478 ops/s` | `-7.48%` | `worse` |
| `segment-index-mixed-drain:partitionedIngestMixed:putWorkload` | `5789.232 ops/s` | `5202.867 ops/s` | `-10.13%` | `worse` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed` | `-` | `198382.674 ops/s` | `-` | `new` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:getWorkload` | `-` | `195689.141 ops/s` | `-` | `new` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:putWorkload` | `-` | `2693.533 ops/s` | `-` | `new` |
