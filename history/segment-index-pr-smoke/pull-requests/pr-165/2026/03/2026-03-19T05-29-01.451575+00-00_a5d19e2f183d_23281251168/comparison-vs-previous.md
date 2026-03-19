# Benchmark Comparison

- Profile: `segment-index-pr-smoke`
- Baseline SHA: `52b04525c10e1f18faddd3a3d7f018427f1a5b53`
- Candidate SHA: `a5d19e2f183d147501f6a9ce9735cbb5b8df27a4`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `segment-index-get-multisegment-hot:getHitAsyncJoin` | `-` | `90.334 ops/s` | `-` | `new` |
| `segment-index-get-multisegment-hot:getHitSync` | `-` | `94.210 ops/s` | `-` | `new` |
| `segment-index-get-multisegment-hot:getMissAsyncJoin` | `-` | `167345.544 ops/s` | `-` | `new` |
| `segment-index-get-multisegment-hot:getMissSync` | `-` | `3699828.231 ops/s` | `-` | `new` |
| `segment-index-get-overlay:getHitAsyncJoin` | `160151.872 ops/s` | `156755.969 ops/s` | `-2.12%` | `neutral` |
| `segment-index-get-overlay:getHitSync` | `4729456.975 ops/s` | `4359336.920 ops/s` | `-7.83%` | `worse` |
| `segment-index-get-overlay:getMissAsyncJoin` | `-` | `166603.150 ops/s` | `-` | `new` |
| `segment-index-get-overlay:getMissSync` | `-` | `3850449.396 ops/s` | `-` | `new` |
| `segment-index-get-persisted:getHitAsyncJoin` | `54557.081 ops/s` | `55708.945 ops/s` | `+2.11%` | `neutral` |
| `segment-index-get-persisted:getHitSync` | `103784.125 ops/s` | `110923.752 ops/s` | `+6.88%` | `better` |
| `segment-index-get-persisted:getMissAsyncJoin` | `-` | `167467.882 ops/s` | `-` | `new` |
| `segment-index-get-persisted:getMissSync` | `-` | `3750372.126 ops/s` | `-` | `new` |
| `segment-index-mixed-drain:partitionedIngestMixed` | `448575.461 ops/s` | `422961.257 ops/s` | `-5.71%` | `warning` |
| `segment-index-mixed-drain:partitionedIngestMixed:getWorkload` | `442786.229 ops/s` | `417796.593 ops/s` | `-5.64%` | `warning` |
| `segment-index-mixed-drain:partitionedIngestMixed:putWorkload` | `5789.232 ops/s` | `5164.663 ops/s` | `-10.79%` | `worse` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed` | `-` | `260686.239 ops/s` | `-` | `new` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:getWorkload` | `-` | `208710.266 ops/s` | `-` | `new` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:putWorkload` | `-` | `51975.973 ops/s` | `-` | `new` |
