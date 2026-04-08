# Benchmark Comparison

- Profile: `segment-index-pr-smoke`
- Baseline SHA: `6482912b40c1a3ba60fd56058c0ca8ed4465cc25`
- Candidate SHA: `932067d3095e951d70ee22cc122c19abacafb3ab`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `segment-index-get-live:getHitAsyncJoin` | `181594.670 ops/s` | `179043.219 ops/s` | `-1.41%` | `neutral` |
| `segment-index-get-live:getHitSync` | `3427952.614 ops/s` | `3443877.870 ops/s` | `+0.46%` | `neutral` |
| `segment-index-get-live:getMissAsyncJoin` | `180052.789 ops/s` | `187782.854 ops/s` | `+4.29%` | `better` |
| `segment-index-get-live:getMissSync` | `3601758.601 ops/s` | `3650205.177 ops/s` | `+1.35%` | `neutral` |
| `segment-index-get-multisegment-hot:getHitAsyncJoin` | `91.361 ops/s` | `92.133 ops/s` | `+0.85%` | `neutral` |
| `segment-index-get-multisegment-hot:getHitSync` | `177.068 ops/s` | `84.720 ops/s` | `-52.15%` | `worse` |
| `segment-index-get-multisegment-hot:getMissAsyncJoin` | `179093.048 ops/s` | `183435.632 ops/s` | `+2.42%` | `neutral` |
| `segment-index-get-multisegment-hot:getMissSync` | `3731262.447 ops/s` | `3740073.109 ops/s` | `+0.24%` | `neutral` |
| `segment-index-get-persisted:getHitAsyncJoin` | `53085.303 ops/s` | `58000.378 ops/s` | `+9.26%` | `better` |
| `segment-index-get-persisted:getHitSync` | `114194.899 ops/s` | `99276.070 ops/s` | `-13.06%` | `worse` |
| `segment-index-get-persisted:getMissAsyncJoin` | `184779.993 ops/s` | `177392.371 ops/s` | `-4.00%` | `warning` |
| `segment-index-get-persisted:getMissSync` | `3615441.872 ops/s` | `3522475.342 ops/s` | `-2.57%` | `neutral` |
| `segment-index-hot-route-put:putHotRoute` | `2482499.751 ops/s` | `2358604.605 ops/s` | `-4.99%` | `warning` |
| `segment-index-hot-route-put:putThenGetHotRoute` | `1355802.309 ops/s` | `1493971.313 ops/s` | `+10.19%` | `better` |
| `segment-index-mixed-drain:partitionedIngestMixed` | `383130.295 ops/s` | `341612.489 ops/s` | `-10.84%` | `worse` |
| `segment-index-mixed-drain:partitionedIngestMixed:getWorkload` | `216520.278 ops/s` | `162622.197 ops/s` | `-24.89%` | `worse` |
| `segment-index-mixed-drain:partitionedIngestMixed:putWorkload` | `166610.016 ops/s` | `178990.293 ops/s` | `+7.43%` | `better` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed` | `45177.464 ops/s` | `43799.156 ops/s` | `-3.05%` | `warning` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:getWorkload` | `37055.440 ops/s` | `38069.908 ops/s` | `+2.74%` | `neutral` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:putWorkload` | `8122.025 ops/s` | `5729.248 ops/s` | `-29.46%` | `worse` |
| `segment-index-persisted-mutation:deleteAsyncJoin` | `3131.405 ops/s` | `3212.422 ops/s` | `+2.59%` | `neutral` |
| `segment-index-persisted-mutation:deleteSync` | `3457.480 ops/s` | `3687.894 ops/s` | `+6.66%` | `better` |
| `segment-index-persisted-mutation:putAsyncJoin` | `3082.004 ops/s` | `2949.389 ops/s` | `-4.30%` | `warning` |
| `segment-index-persisted-mutation:putSync` | `3604.050 ops/s` | `3383.133 ops/s` | `-6.13%` | `warning` |
