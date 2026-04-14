# Benchmark Comparison

- Profile: `segment-index-pr-smoke`
- Baseline SHA: `ff40d70c41db5d6bfb2af076123dcfe2714e0f38`
- Candidate SHA: `e43cc9e2c380a01ebaff9b63a49b87068d44a273`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `segment-index-get-live:getHitSync` | `3643924.223 ops/s` | `3473883.602 ops/s` | `-4.67%` | `warning` |
| `segment-index-get-live:getMissSync` | `4161803.563 ops/s` | `4020369.910 ops/s` | `-3.40%` | `warning` |
| `segment-index-get-multisegment-hot:getHitSync` | `74.964 ops/s` | `86.092 ops/s` | `+14.84%` | `better` |
| `segment-index-get-multisegment-hot:getMissSync` | `3747455.828 ops/s` | `3809801.440 ops/s` | `+1.66%` | `neutral` |
| `segment-index-get-persisted:getHitSync` | `113825.669 ops/s` | `122942.026 ops/s` | `+8.01%` | `better` |
| `segment-index-get-persisted:getMissSync` | `4111937.640 ops/s` | `4232887.733 ops/s` | `+2.94%` | `neutral` |
| `segment-index-hot-route-put:putHotRoute` | `2506909.511 ops/s` | `2421122.566 ops/s` | `-3.42%` | `warning` |
| `segment-index-hot-route-put:putThenGetHotRoute` | `1568174.160 ops/s` | `1574681.202 ops/s` | `+0.41%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed` | `351799.490 ops/s` | `376911.730 ops/s` | `+7.14%` | `better` |
| `segment-index-mixed-drain:partitionedIngestMixed:getWorkload` | `190293.199 ops/s` | `214448.989 ops/s` | `+12.69%` | `better` |
| `segment-index-mixed-drain:partitionedIngestMixed:putWorkload` | `161506.291 ops/s` | `162462.742 ops/s` | `+0.59%` | `neutral` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed` | `42177.377 ops/s` | `35669.801 ops/s` | `-15.43%` | `worse` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:getWorkload` | `33154.657 ops/s` | `30216.110 ops/s` | `-8.86%` | `worse` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:putWorkload` | `9022.720 ops/s` | `5453.691 ops/s` | `-39.56%` | `worse` |
| `segment-index-persisted-mutation:deleteSync` | `2115.406 ops/s` | `2087.006 ops/s` | `-1.34%` | `neutral` |
| `segment-index-persisted-mutation:putSync` | `1969.544 ops/s` | `2014.080 ops/s` | `+2.26%` | `neutral` |
