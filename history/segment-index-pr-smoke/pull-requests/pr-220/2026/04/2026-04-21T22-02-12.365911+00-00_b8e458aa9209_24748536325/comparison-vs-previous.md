# Benchmark Comparison

- Profile: `segment-index-pr-smoke`
- Baseline SHA: `91ac90ad766133e4d0f5e5ea83c3ccf14cbd8b50`
- Candidate SHA: `b8e458aa920967b0a677589b9cdfeac52bc6d7bf`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `segment-index-get-live:getHitSync` | `2115249.704 ops/s` | `1993304.465 ops/s` | `-5.77%` | `warning` |
| `segment-index-get-live:getMissSync` | `3992851.926 ops/s` | `3766354.577 ops/s` | `-5.67%` | `warning` |
| `segment-index-get-multisegment-hot:getHitSync` | `91.208 ops/s` | `7922.040 ops/s` | `+8585.66%` | `better` |
| `segment-index-get-multisegment-hot:getMissSync` | `3560671.734 ops/s` | `3926995.886 ops/s` | `+10.29%` | `better` |
| `segment-index-get-persisted:getHitSync` | `113416.421 ops/s` | `119837.532 ops/s` | `+5.66%` | `better` |
| `segment-index-get-persisted:getMissSync` | `3953496.614 ops/s` | `3925926.777 ops/s` | `-0.70%` | `neutral` |
| `segment-index-hot-route-put:putHotRoute` | `2187233.430 ops/s` | `2207719.293 ops/s` | `+0.94%` | `neutral` |
| `segment-index-hot-route-put:putThenGetHotRoute` | `1086221.984 ops/s` | `1101483.650 ops/s` | `+1.41%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed` | `309693.296 ops/s` | `302197.021 ops/s` | `-2.42%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed:getWorkload` | `147151.891 ops/s` | `140715.507 ops/s` | `-4.37%` | `warning` |
| `segment-index-mixed-drain:partitionedIngestMixed:putWorkload` | `162541.405 ops/s` | `161481.514 ops/s` | `-0.65%` | `neutral` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed` | `9236.676 ops/s` | `85062.547 ops/s` | `+820.92%` | `better` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:getWorkload` | `2810.278 ops/s` | `74987.649 ops/s` | `+2568.34%` | `better` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:putWorkload` | `6426.398 ops/s` | `10074.899 ops/s` | `+56.77%` | `better` |
| `segment-index-persisted-mutation:deleteSync` | `2601.533 ops/s` | `2645.510 ops/s` | `+1.69%` | `neutral` |
| `segment-index-persisted-mutation:putSync` | `2582.562 ops/s` | `2573.549 ops/s` | `-0.35%` | `neutral` |
