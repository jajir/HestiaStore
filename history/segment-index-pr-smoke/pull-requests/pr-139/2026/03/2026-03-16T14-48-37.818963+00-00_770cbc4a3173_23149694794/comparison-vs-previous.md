# Benchmark Comparison

- Profile: `segment-index-pr-smoke`
- Baseline SHA: `52b04525c10e1f18faddd3a3d7f018427f1a5b53`
- Candidate SHA: `770cbc4a3173c008df4d18c12a4d7c1db9e71e23`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `segment-index-get-multisegment-hot:getHitAsyncJoin` | `-` | `99.789 ops/s` | `-` | `new` |
| `segment-index-get-multisegment-hot:getHitSync` | `-` | `102.413 ops/s` | `-` | `new` |
| `segment-index-get-multisegment-hot:getMissAsyncJoin` | `-` | `176094.679 ops/s` | `-` | `new` |
| `segment-index-get-multisegment-hot:getMissSync` | `-` | `7068983.951 ops/s` | `-` | `new` |
| `segment-index-get-overlay:getHitAsyncJoin` | `160151.872 ops/s` | `165343.187 ops/s` | `+3.24%` | `better` |
| `segment-index-get-overlay:getHitSync` | `4729456.975 ops/s` | `5437658.416 ops/s` | `+14.97%` | `better` |
| `segment-index-get-overlay:getMissAsyncJoin` | `-` | `169329.369 ops/s` | `-` | `new` |
| `segment-index-get-overlay:getMissSync` | `-` | `6765782.325 ops/s` | `-` | `new` |
| `segment-index-get-persisted:getHitAsyncJoin` | `54557.081 ops/s` | `58651.305 ops/s` | `+7.50%` | `better` |
| `segment-index-get-persisted:getHitSync` | `103784.125 ops/s` | `112844.954 ops/s` | `+8.73%` | `better` |
| `segment-index-get-persisted:getMissAsyncJoin` | `-` | `177029.439 ops/s` | `-` | `new` |
| `segment-index-get-persisted:getMissSync` | `-` | `6881320.693 ops/s` | `-` | `new` |
| `segment-index-mixed-drain:partitionedIngestMixed` | `448575.461 ops/s` | `453170.162 ops/s` | `+1.02%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed:getWorkload` | `442786.229 ops/s` | `447137.603 ops/s` | `+0.98%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed:putWorkload` | `5789.232 ops/s` | `6032.559 ops/s` | `+4.20%` | `better` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed` | `-` | `203398.293 ops/s` | `-` | `new` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:getWorkload` | `-` | `200599.294 ops/s` | `-` | `new` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:putWorkload` | `-` | `2798.999 ops/s` | `-` | `new` |
