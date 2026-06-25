# Benchmark Comparison

- Profile: `segment-index-pr-smoke`
- Baseline SHA: `240bde3d9176c1a1ab1fcd7a76c939364cdded57`
- Candidate SHA: `7c89d40c506b4a34ac676f7212ef7276c51e7308`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `segment-index-get-live:getHitSync` | `2252712.682 ops/s` | `2146186.881 ops/s` | `-4.73%` | `warning` |
| `segment-index-get-live:getMissSync` | `1985675.997 ops/s` | `2045380.358 ops/s` | `+3.01%` | `better` |
| `segment-index-get-persisted:getHitSync` | `2034591.749 ops/s` | `1940084.151 ops/s` | `-4.65%` | `warning` |
| `segment-index-get-persisted:getMissSync` | `2079635.353 ops/s` | `2018334.995 ops/s` | `-2.95%` | `neutral` |
| `segment-index-hot-route-put:putHotRoute` | `2092056.333 ops/s` | `2076073.705 ops/s` | `-0.76%` | `neutral` |
| `segment-index-hot-route-put:putThenGetHotRoute` | `1087131.709 ops/s` | `1100984.378 ops/s` | `+1.27%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed` | `324019.737 ops/s` | `301176.372 ops/s` | `-7.05%` | `worse` |
| `segment-index-mixed-drain:partitionedIngestMixed:getWorkload` | `152344.157 ops/s` | `135530.108 ops/s` | `-11.04%` | `worse` |
| `segment-index-mixed-drain:partitionedIngestMixed:putWorkload` | `171675.579 ops/s` | `165646.265 ops/s` | `-3.51%` | `warning` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed` | `197811.891 ops/s` | `196204.788 ops/s` | `-0.81%` | `neutral` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:getWorkload` | `184291.659 ops/s` | `181863.391 ops/s` | `-1.32%` | `neutral` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:putWorkload` | `13520.233 ops/s` | `14341.397 ops/s` | `+6.07%` | `better` |
