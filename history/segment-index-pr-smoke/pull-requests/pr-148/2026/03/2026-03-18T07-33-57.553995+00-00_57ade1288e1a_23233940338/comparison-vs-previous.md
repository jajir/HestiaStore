# Benchmark Comparison

- Profile: `segment-index-pr-smoke`
- Baseline SHA: `52b04525c10e1f18faddd3a3d7f018427f1a5b53`
- Candidate SHA: `57ade1288e1a445855cae6521a444a64634b7fcc`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `segment-index-get-multisegment-hot:getHitAsyncJoin` | `-` | `97.223 ops/s` | `-` | `new` |
| `segment-index-get-multisegment-hot:getHitSync` | `-` | `95.522 ops/s` | `-` | `new` |
| `segment-index-get-multisegment-hot:getMissAsyncJoin` | `-` | `165943.056 ops/s` | `-` | `new` |
| `segment-index-get-multisegment-hot:getMissSync` | `-` | `5962737.773 ops/s` | `-` | `new` |
| `segment-index-get-overlay:getHitAsyncJoin` | `160151.872 ops/s` | `163844.825 ops/s` | `+2.31%` | `neutral` |
| `segment-index-get-overlay:getHitSync` | `4729456.975 ops/s` | `5143423.109 ops/s` | `+8.75%` | `better` |
| `segment-index-get-overlay:getMissAsyncJoin` | `-` | `166555.833 ops/s` | `-` | `new` |
| `segment-index-get-overlay:getMissSync` | `-` | `6752025.785 ops/s` | `-` | `new` |
| `segment-index-get-persisted:getHitAsyncJoin` | `54557.081 ops/s` | `56381.533 ops/s` | `+3.34%` | `better` |
| `segment-index-get-persisted:getHitSync` | `103784.125 ops/s` | `109254.185 ops/s` | `+5.27%` | `better` |
| `segment-index-get-persisted:getMissAsyncJoin` | `-` | `167982.016 ops/s` | `-` | `new` |
| `segment-index-get-persisted:getMissSync` | `-` | `6845589.126 ops/s` | `-` | `new` |
| `segment-index-mixed-drain:partitionedIngestMixed` | `448575.461 ops/s` | `454987.673 ops/s` | `+1.43%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed:getWorkload` | `442786.229 ops/s` | `449130.891 ops/s` | `+1.43%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed:putWorkload` | `5789.232 ops/s` | `5856.782 ops/s` | `+1.17%` | `neutral` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed` | `-` | `206144.565 ops/s` | `-` | `new` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:getWorkload` | `-` | `203373.156 ops/s` | `-` | `new` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:putWorkload` | `-` | `2771.409 ops/s` | `-` | `new` |
