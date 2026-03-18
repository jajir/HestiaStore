# Benchmark Comparison

- Profile: `segment-index-pr-smoke`
- Baseline SHA: `52b04525c10e1f18faddd3a3d7f018427f1a5b53`
- Candidate SHA: `3039d77e58be44238233b282afc4014149806874`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `segment-index-get-multisegment-hot:getHitAsyncJoin` | `-` | `102.052 ops/s` | `-` | `new` |
| `segment-index-get-multisegment-hot:getHitSync` | `-` | `95.745 ops/s` | `-` | `new` |
| `segment-index-get-multisegment-hot:getMissAsyncJoin` | `-` | `239328.233 ops/s` | `-` | `new` |
| `segment-index-get-multisegment-hot:getMissSync` | `-` | `4660150.083 ops/s` | `-` | `new` |
| `segment-index-get-overlay:getHitAsyncJoin` | `160151.872 ops/s` | `234762.686 ops/s` | `+46.59%` | `better` |
| `segment-index-get-overlay:getHitSync` | `4729456.975 ops/s` | `3494545.897 ops/s` | `-26.11%` | `worse` |
| `segment-index-get-overlay:getMissAsyncJoin` | `-` | `232790.896 ops/s` | `-` | `new` |
| `segment-index-get-overlay:getMissSync` | `-` | `4767865.704 ops/s` | `-` | `new` |
| `segment-index-get-persisted:getHitAsyncJoin` | `54557.081 ops/s` | `75200.214 ops/s` | `+37.84%` | `better` |
| `segment-index-get-persisted:getHitSync` | `103784.125 ops/s` | `103616.354 ops/s` | `-0.16%` | `neutral` |
| `segment-index-get-persisted:getMissAsyncJoin` | `-` | `238640.911 ops/s` | `-` | `new` |
| `segment-index-get-persisted:getMissSync` | `-` | `4939608.926 ops/s` | `-` | `new` |
| `segment-index-mixed-drain:partitionedIngestMixed` | `448575.461 ops/s` | `363163.216 ops/s` | `-19.04%` | `worse` |
| `segment-index-mixed-drain:partitionedIngestMixed:getWorkload` | `442786.229 ops/s` | `357328.087 ops/s` | `-19.30%` | `worse` |
| `segment-index-mixed-drain:partitionedIngestMixed:putWorkload` | `5789.232 ops/s` | `5835.129 ops/s` | `+0.79%` | `neutral` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed` | `-` | `208408.949 ops/s` | `-` | `new` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:getWorkload` | `-` | `205800.919 ops/s` | `-` | `new` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:putWorkload` | `-` | `2608.029 ops/s` | `-` | `new` |
