# Benchmark Comparison

- Profile: `segment-index-pr-smoke`
- Baseline SHA: `52b04525c10e1f18faddd3a3d7f018427f1a5b53`
- Candidate SHA: `ac83b9c5408a61f0c873f57025ba1639ab749d97`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `segment-index-get-multisegment-hot:getHitAsyncJoin` | `-` | `104.658 ops/s` | `-` | `new` |
| `segment-index-get-multisegment-hot:getHitSync` | `-` | `91.795 ops/s` | `-` | `new` |
| `segment-index-get-multisegment-hot:getMissAsyncJoin` | `-` | `175365.001 ops/s` | `-` | `new` |
| `segment-index-get-multisegment-hot:getMissSync` | `-` | `6278613.455 ops/s` | `-` | `new` |
| `segment-index-get-overlay:getHitAsyncJoin` | `160151.872 ops/s` | `168346.391 ops/s` | `+5.12%` | `better` |
| `segment-index-get-overlay:getHitSync` | `4729456.975 ops/s` | `5590755.259 ops/s` | `+18.21%` | `better` |
| `segment-index-get-overlay:getMissAsyncJoin` | `-` | `168058.061 ops/s` | `-` | `new` |
| `segment-index-get-overlay:getMissSync` | `-` | `6737980.395 ops/s` | `-` | `new` |
| `segment-index-get-persisted:getHitAsyncJoin` | `54557.081 ops/s` | `59010.232 ops/s` | `+8.16%` | `better` |
| `segment-index-get-persisted:getHitSync` | `103784.125 ops/s` | `103112.840 ops/s` | `-0.65%` | `neutral` |
| `segment-index-get-persisted:getMissAsyncJoin` | `-` | `174748.243 ops/s` | `-` | `new` |
| `segment-index-get-persisted:getMissSync` | `-` | `6751146.954 ops/s` | `-` | `new` |
| `segment-index-mixed-drain:partitionedIngestMixed` | `448575.461 ops/s` | `440097.711 ops/s` | `-1.89%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed:getWorkload` | `442786.229 ops/s` | `434327.976 ops/s` | `-1.91%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed:putWorkload` | `5789.232 ops/s` | `5769.734 ops/s` | `-0.34%` | `neutral` |
