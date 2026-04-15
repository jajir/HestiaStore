# Benchmark Comparison

- Profile: `segment-index-pr-smoke`
- Baseline SHA: `30f93a9be86df7361bbeb5cf93c3547cbb155d00`
- Candidate SHA: `824878b44a20c50c1a7dc66da328fa39854797b7`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `segment-index-get-live:getHitSync` | `3589569.526 ops/s` | `3441162.969 ops/s` | `-4.13%` | `warning` |
| `segment-index-get-live:getMissSync` | `4111043.082 ops/s` | `4002046.364 ops/s` | `-2.65%` | `neutral` |
| `segment-index-get-multisegment-hot:getHitSync` | `94.254 ops/s` | `92.614 ops/s` | `-1.74%` | `neutral` |
| `segment-index-get-multisegment-hot:getMissSync` | `3758754.220 ops/s` | `3361848.243 ops/s` | `-10.56%` | `worse` |
| `segment-index-get-persisted:getHitSync` | `105735.365 ops/s` | `124370.018 ops/s` | `+17.62%` | `better` |
| `segment-index-get-persisted:getMissSync` | `4220139.713 ops/s` | `3957412.489 ops/s` | `-6.23%` | `warning` |
| `segment-index-hot-route-put:putHotRoute` | `2344109.108 ops/s` | `2374351.787 ops/s` | `+1.29%` | `neutral` |
| `segment-index-hot-route-put:putThenGetHotRoute` | `1490140.886 ops/s` | `1554676.239 ops/s` | `+4.33%` | `better` |
| `segment-index-mixed-drain:partitionedIngestMixed` | `366578.635 ops/s` | `358300.025 ops/s` | `-2.26%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed:getWorkload` | `194091.089 ops/s` | `180578.382 ops/s` | `-6.96%` | `warning` |
| `segment-index-mixed-drain:partitionedIngestMixed:putWorkload` | `172487.547 ops/s` | `177721.643 ops/s` | `+3.03%` | `better` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed` | `31309.318 ops/s` | `18548.798 ops/s` | `-40.76%` | `worse` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:getWorkload` | `19793.849 ops/s` | `11451.449 ops/s` | `-42.15%` | `worse` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:putWorkload` | `11515.469 ops/s` | `7097.349 ops/s` | `-38.37%` | `worse` |
| `segment-index-persisted-mutation:deleteSync` | `1859.971 ops/s` | `1601.592 ops/s` | `-13.89%` | `worse` |
| `segment-index-persisted-mutation:putSync` | `1984.876 ops/s` | `1647.792 ops/s` | `-16.98%` | `worse` |
