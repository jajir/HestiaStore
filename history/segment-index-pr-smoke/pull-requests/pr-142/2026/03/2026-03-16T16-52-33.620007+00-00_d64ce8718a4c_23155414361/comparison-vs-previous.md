# Benchmark Comparison

- Profile: `segment-index-pr-smoke`
- Baseline SHA: `52b04525c10e1f18faddd3a3d7f018427f1a5b53`
- Candidate SHA: `d64ce8718a4c762820548bd5ecd2d40f904cca4b`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `segment-index-get-multisegment-hot:getHitAsyncJoin` | `-` | `104.660 ops/s` | `-` | `new` |
| `segment-index-get-multisegment-hot:getHitSync` | `-` | `93.897 ops/s` | `-` | `new` |
| `segment-index-get-multisegment-hot:getMissAsyncJoin` | `-` | `175593.325 ops/s` | `-` | `new` |
| `segment-index-get-multisegment-hot:getMissSync` | `-` | `6517875.559 ops/s` | `-` | `new` |
| `segment-index-get-overlay:getHitAsyncJoin` | `160151.872 ops/s` | `165638.573 ops/s` | `+3.43%` | `better` |
| `segment-index-get-overlay:getHitSync` | `4729456.975 ops/s` | `5365546.600 ops/s` | `+13.45%` | `better` |
| `segment-index-get-overlay:getMissAsyncJoin` | `-` | `175472.953 ops/s` | `-` | `new` |
| `segment-index-get-overlay:getMissSync` | `-` | `6954912.458 ops/s` | `-` | `new` |
| `segment-index-get-persisted:getHitAsyncJoin` | `54557.081 ops/s` | `59796.041 ops/s` | `+9.60%` | `better` |
| `segment-index-get-persisted:getHitSync` | `103784.125 ops/s` | `112740.395 ops/s` | `+8.63%` | `better` |
| `segment-index-get-persisted:getMissAsyncJoin` | `-` | `175786.971 ops/s` | `-` | `new` |
| `segment-index-get-persisted:getMissSync` | `-` | `6823152.962 ops/s` | `-` | `new` |
| `segment-index-mixed-drain:partitionedIngestMixed` | `448575.461 ops/s` | `492454.992 ops/s` | `+9.78%` | `better` |
| `segment-index-mixed-drain:partitionedIngestMixed:getWorkload` | `442786.229 ops/s` | `486516.987 ops/s` | `+9.88%` | `better` |
| `segment-index-mixed-drain:partitionedIngestMixed:putWorkload` | `5789.232 ops/s` | `5938.005 ops/s` | `+2.57%` | `neutral` |
