# Benchmark Comparison

- Profile: `segment-index-pr-smoke`
- Baseline SHA: `51724b114025e9af1cae85d0e87d4678c8b87310`
- Candidate SHA: `a77ef7ba1d016d05408de5ac088ac29561f22251`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `segment-index-get-live:getHitSync` | `3367217.608 ops/s` | `3300488.297 ops/s` | `-1.98%` | `neutral` |
| `segment-index-get-live:getMissSync` | `4106192.579 ops/s` | `4103415.254 ops/s` | `-0.07%` | `neutral` |
| `segment-index-get-multisegment-hot:getHitSync` | `90.562 ops/s` | `89.468 ops/s` | `-1.21%` | `neutral` |
| `segment-index-get-multisegment-hot:getMissSync` | `3642650.790 ops/s` | `3805112.770 ops/s` | `+4.46%` | `better` |
| `segment-index-get-persisted:getHitSync` | `123361.104 ops/s` | `123624.779 ops/s` | `+0.21%` | `neutral` |
| `segment-index-get-persisted:getMissSync` | `4115144.054 ops/s` | `3947066.745 ops/s` | `-4.08%` | `warning` |
| `segment-index-hot-route-put:putHotRoute` | `2319346.689 ops/s` | `2305576.225 ops/s` | `-0.59%` | `neutral` |
| `segment-index-hot-route-put:putThenGetHotRoute` | `1273575.021 ops/s` | `1540174.694 ops/s` | `+20.93%` | `better` |
| `segment-index-mixed-drain:partitionedIngestMixed` | `339307.221 ops/s` | `339938.513 ops/s` | `+0.19%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed:getWorkload` | `182362.114 ops/s` | `171274.939 ops/s` | `-6.08%` | `warning` |
| `segment-index-mixed-drain:partitionedIngestMixed:putWorkload` | `156945.107 ops/s` | `168663.574 ops/s` | `+7.47%` | `better` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed` | `112572.865 ops/s` | `22886.187 ops/s` | `-79.67%` | `worse` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:getWorkload` | `109959.369 ops/s` | `518.362 ops/s` | `-99.53%` | `worse` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:putWorkload` | `2613.495 ops/s` | `22367.825 ops/s` | `+755.86%` | `better` |
| `segment-index-persisted-mutation:deleteSync` | `2658.672 ops/s` | `2676.783 ops/s` | `+0.68%` | `neutral` |
| `segment-index-persisted-mutation:putSync` | `2638.899 ops/s` | `2463.018 ops/s` | `-6.66%` | `warning` |
