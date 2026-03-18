# Benchmark Comparison

- Profile: `segment-index-pr-smoke`
- Baseline SHA: `52b04525c10e1f18faddd3a3d7f018427f1a5b53`
- Candidate SHA: `bd1eb1115aa9ce7f6fbad23cc4fd897878df59b5`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `segment-index-get-multisegment-hot:getHitAsyncJoin` | `-` | `98.778 ops/s` | `-` | `new` |
| `segment-index-get-multisegment-hot:getHitSync` | `-` | `102.909 ops/s` | `-` | `new` |
| `segment-index-get-multisegment-hot:getMissAsyncJoin` | `-` | `170869.447 ops/s` | `-` | `new` |
| `segment-index-get-multisegment-hot:getMissSync` | `-` | `5978889.519 ops/s` | `-` | `new` |
| `segment-index-get-overlay:getHitAsyncJoin` | `160151.872 ops/s` | `168972.670 ops/s` | `+5.51%` | `better` |
| `segment-index-get-overlay:getHitSync` | `4729456.975 ops/s` | `5074856.663 ops/s` | `+7.30%` | `better` |
| `segment-index-get-overlay:getMissAsyncJoin` | `-` | `161510.609 ops/s` | `-` | `new` |
| `segment-index-get-overlay:getMissSync` | `-` | `7298603.098 ops/s` | `-` | `new` |
| `segment-index-get-persisted:getHitAsyncJoin` | `54557.081 ops/s` | `57342.434 ops/s` | `+5.11%` | `better` |
| `segment-index-get-persisted:getHitSync` | `103784.125 ops/s` | `109919.764 ops/s` | `+5.91%` | `better` |
| `segment-index-get-persisted:getMissAsyncJoin` | `-` | `164789.104 ops/s` | `-` | `new` |
| `segment-index-get-persisted:getMissSync` | `-` | `6412748.652 ops/s` | `-` | `new` |
| `segment-index-mixed-drain:partitionedIngestMixed` | `448575.461 ops/s` | `456336.555 ops/s` | `+1.73%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed:getWorkload` | `442786.229 ops/s` | `450656.561 ops/s` | `+1.78%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed:putWorkload` | `5789.232 ops/s` | `5679.994 ops/s` | `-1.89%` | `neutral` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed` | `-` | `215367.312 ops/s` | `-` | `new` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:getWorkload` | `-` | `212730.679 ops/s` | `-` | `new` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:putWorkload` | `-` | `2636.633 ops/s` | `-` | `new` |
