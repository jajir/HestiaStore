# Benchmark Comparison

- Profile: `segment-index-pr-smoke`
- Baseline SHA: `52b04525c10e1f18faddd3a3d7f018427f1a5b53`
- Candidate SHA: `82087155c6888c3fb3b6f62f577a310c2bbeb928`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `segment-index-get-multisegment-hot:getHitAsyncJoin` | `-` | `99.425 ops/s` | `-` | `new` |
| `segment-index-get-multisegment-hot:getHitSync` | `-` | `97.969 ops/s` | `-` | `new` |
| `segment-index-get-multisegment-hot:getMissAsyncJoin` | `-` | `165034.056 ops/s` | `-` | `new` |
| `segment-index-get-multisegment-hot:getMissSync` | `-` | `3978303.747 ops/s` | `-` | `new` |
| `segment-index-get-overlay:getHitAsyncJoin` | `160151.872 ops/s` | `155222.766 ops/s` | `-3.08%` | `warning` |
| `segment-index-get-overlay:getHitSync` | `4729456.975 ops/s` | `4166629.516 ops/s` | `-11.90%` | `worse` |
| `segment-index-get-overlay:getMissAsyncJoin` | `-` | `162497.659 ops/s` | `-` | `new` |
| `segment-index-get-overlay:getMissSync` | `-` | `4243216.200 ops/s` | `-` | `new` |
| `segment-index-get-persisted:getHitAsyncJoin` | `54557.081 ops/s` | `58722.661 ops/s` | `+7.64%` | `better` |
| `segment-index-get-persisted:getHitSync` | `103784.125 ops/s` | `102096.520 ops/s` | `-1.63%` | `neutral` |
| `segment-index-get-persisted:getMissAsyncJoin` | `-` | `164565.755 ops/s` | `-` | `new` |
| `segment-index-get-persisted:getMissSync` | `-` | `3961570.361 ops/s` | `-` | `new` |
| `segment-index-mixed-drain:partitionedIngestMixed` | `448575.461 ops/s` | `493817.020 ops/s` | `+10.09%` | `better` |
| `segment-index-mixed-drain:partitionedIngestMixed:getWorkload` | `442786.229 ops/s` | `487909.073 ops/s` | `+10.19%` | `better` |
| `segment-index-mixed-drain:partitionedIngestMixed:putWorkload` | `5789.232 ops/s` | `5907.947 ops/s` | `+2.05%` | `neutral` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed` | `-` | `197362.798 ops/s` | `-` | `new` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:getWorkload` | `-` | `194640.886 ops/s` | `-` | `new` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:putWorkload` | `-` | `2721.912 ops/s` | `-` | `new` |
