# Benchmark Comparison

- Profile: `segment-index-pr-smoke`
- Baseline SHA: `52b04525c10e1f18faddd3a3d7f018427f1a5b53`
- Candidate SHA: `0bb3fe12f0365a0e9bcf48bebef5baecb64ecb47`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `segment-index-get-multisegment-hot:getHitAsyncJoin` | `-` | `94.927 ops/s` | `-` | `new` |
| `segment-index-get-multisegment-hot:getHitSync` | `-` | `98.010 ops/s` | `-` | `new` |
| `segment-index-get-multisegment-hot:getMissAsyncJoin` | `-` | `177198.032 ops/s` | `-` | `new` |
| `segment-index-get-multisegment-hot:getMissSync` | `-` | `6021752.461 ops/s` | `-` | `new` |
| `segment-index-get-overlay:getHitAsyncJoin` | `160151.872 ops/s` | `172502.334 ops/s` | `+7.71%` | `better` |
| `segment-index-get-overlay:getHitSync` | `4729456.975 ops/s` | `5222113.266 ops/s` | `+10.42%` | `better` |
| `segment-index-get-overlay:getMissAsyncJoin` | `-` | `176651.688 ops/s` | `-` | `new` |
| `segment-index-get-overlay:getMissSync` | `-` | `6799043.568 ops/s` | `-` | `new` |
| `segment-index-get-persisted:getHitAsyncJoin` | `54557.081 ops/s` | `59067.032 ops/s` | `+8.27%` | `better` |
| `segment-index-get-persisted:getHitSync` | `103784.125 ops/s` | `109763.207 ops/s` | `+5.76%` | `better` |
| `segment-index-get-persisted:getMissAsyncJoin` | `-` | `177759.995 ops/s` | `-` | `new` |
| `segment-index-get-persisted:getMissSync` | `-` | `7258178.148 ops/s` | `-` | `new` |
| `segment-index-mixed-drain:partitionedIngestMixed` | `448575.461 ops/s` | `479849.631 ops/s` | `+6.97%` | `better` |
| `segment-index-mixed-drain:partitionedIngestMixed:getWorkload` | `442786.229 ops/s` | `473943.761 ops/s` | `+7.04%` | `better` |
| `segment-index-mixed-drain:partitionedIngestMixed:putWorkload` | `5789.232 ops/s` | `5905.871 ops/s` | `+2.01%` | `neutral` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed` | `-` | `204546.047 ops/s` | `-` | `new` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:getWorkload` | `-` | `201876.370 ops/s` | `-` | `new` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:putWorkload` | `-` | `2669.678 ops/s` | `-` | `new` |
