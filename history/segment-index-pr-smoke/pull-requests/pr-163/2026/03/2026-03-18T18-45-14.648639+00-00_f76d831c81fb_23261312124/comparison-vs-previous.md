# Benchmark Comparison

- Profile: `segment-index-pr-smoke`
- Baseline SHA: `52b04525c10e1f18faddd3a3d7f018427f1a5b53`
- Candidate SHA: `f76d831c81fbb1a4ba0dcaa69ea228a124506a99`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `segment-index-get-multisegment-hot:getHitAsyncJoin` | `-` | `98.641 ops/s` | `-` | `new` |
| `segment-index-get-multisegment-hot:getHitSync` | `-` | `91.192 ops/s` | `-` | `new` |
| `segment-index-get-multisegment-hot:getMissAsyncJoin` | `-` | `177804.068 ops/s` | `-` | `new` |
| `segment-index-get-multisegment-hot:getMissSync` | `-` | `6700951.966 ops/s` | `-` | `new` |
| `segment-index-get-overlay:getHitAsyncJoin` | `160151.872 ops/s` | `173041.520 ops/s` | `+8.05%` | `better` |
| `segment-index-get-overlay:getHitSync` | `4729456.975 ops/s` | `5338542.483 ops/s` | `+12.88%` | `better` |
| `segment-index-get-overlay:getMissAsyncJoin` | `-` | `174947.072 ops/s` | `-` | `new` |
| `segment-index-get-overlay:getMissSync` | `-` | `6949479.132 ops/s` | `-` | `new` |
| `segment-index-get-persisted:getHitAsyncJoin` | `54557.081 ops/s` | `61762.429 ops/s` | `+13.21%` | `better` |
| `segment-index-get-persisted:getHitSync` | `103784.125 ops/s` | `113809.769 ops/s` | `+9.66%` | `better` |
| `segment-index-get-persisted:getMissAsyncJoin` | `-` | `172162.943 ops/s` | `-` | `new` |
| `segment-index-get-persisted:getMissSync` | `-` | `7020172.042 ops/s` | `-` | `new` |
| `segment-index-mixed-drain:partitionedIngestMixed` | `448575.461 ops/s` | `425252.920 ops/s` | `-5.20%` | `warning` |
| `segment-index-mixed-drain:partitionedIngestMixed:getWorkload` | `442786.229 ops/s` | `419301.020 ops/s` | `-5.30%` | `warning` |
| `segment-index-mixed-drain:partitionedIngestMixed:putWorkload` | `5789.232 ops/s` | `5951.900 ops/s` | `+2.81%` | `neutral` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed` | `-` | `202857.271 ops/s` | `-` | `new` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:getWorkload` | `-` | `200412.710 ops/s` | `-` | `new` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:putWorkload` | `-` | `2444.561 ops/s` | `-` | `new` |
