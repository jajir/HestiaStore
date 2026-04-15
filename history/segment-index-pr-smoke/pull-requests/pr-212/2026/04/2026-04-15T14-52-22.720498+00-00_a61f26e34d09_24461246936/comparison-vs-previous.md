# Benchmark Comparison

- Profile: `segment-index-pr-smoke`
- Baseline SHA: `30f93a9be86df7361bbeb5cf93c3547cbb155d00`
- Candidate SHA: `a61f26e34d09cc5b31f4919fba6ea5f9e08210c7`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `segment-index-get-live:getHitSync` | `3484959.792 ops/s` | `3515175.008 ops/s` | `+0.87%` | `neutral` |
| `segment-index-get-live:getMissSync` | `3957479.798 ops/s` | `3624585.114 ops/s` | `-8.41%` | `worse` |
| `segment-index-get-multisegment-hot:getHitSync` | `85.907 ops/s` | `92.473 ops/s` | `+7.64%` | `better` |
| `segment-index-get-multisegment-hot:getMissSync` | `4007808.498 ops/s` | `3652883.510 ops/s` | `-8.86%` | `worse` |
| `segment-index-get-persisted:getHitSync` | `110313.576 ops/s` | `117002.586 ops/s` | `+6.06%` | `better` |
| `segment-index-get-persisted:getMissSync` | `3930556.875 ops/s` | `3757926.026 ops/s` | `-4.39%` | `warning` |
| `segment-index-hot-route-put:putHotRoute` | `2693984.913 ops/s` | `2508865.885 ops/s` | `-6.87%` | `warning` |
| `segment-index-hot-route-put:putThenGetHotRoute` | `1513378.860 ops/s` | `1507319.822 ops/s` | `-0.40%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed` | `355089.068 ops/s` | `341139.764 ops/s` | `-3.93%` | `warning` |
| `segment-index-mixed-drain:partitionedIngestMixed:getWorkload` | `179624.870 ops/s` | `168098.303 ops/s` | `-6.42%` | `warning` |
| `segment-index-mixed-drain:partitionedIngestMixed:putWorkload` | `175464.198 ops/s` | `173041.462 ops/s` | `-1.38%` | `neutral` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed` | `46917.871 ops/s` | `36399.137 ops/s` | `-22.42%` | `worse` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:getWorkload` | `36368.703 ops/s` | `31444.616 ops/s` | `-13.54%` | `worse` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:putWorkload` | `10549.169 ops/s` | `4954.521 ops/s` | `-53.03%` | `worse` |
| `segment-index-persisted-mutation:deleteSync` | `3487.514 ops/s` | `3396.778 ops/s` | `-2.60%` | `neutral` |
| `segment-index-persisted-mutation:putSync` | `3484.696 ops/s` | `3585.334 ops/s` | `+2.89%` | `neutral` |
