# Benchmark Comparison

- Profile: `segment-index-pr-smoke`
- Baseline SHA: `30f93a9be86df7361bbeb5cf93c3547cbb155d00`
- Candidate SHA: `21aa9dcb8725efa16ce4d165ddf8cd62bec4264e`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `segment-index-get-live:getHitSync` | `3691832.399 ops/s` | `3672882.802 ops/s` | `-0.51%` | `neutral` |
| `segment-index-get-live:getMissSync` | `3984698.089 ops/s` | `4098660.572 ops/s` | `+2.86%` | `neutral` |
| `segment-index-get-multisegment-hot:getHitSync` | `92.855 ops/s` | `85.384 ops/s` | `-8.05%` | `worse` |
| `segment-index-get-multisegment-hot:getMissSync` | `4006873.251 ops/s` | `3613779.839 ops/s` | `-9.81%` | `worse` |
| `segment-index-get-persisted:getHitSync` | `113135.055 ops/s` | `125527.675 ops/s` | `+10.95%` | `better` |
| `segment-index-get-persisted:getMissSync` | `4218816.399 ops/s` | `4145328.694 ops/s` | `-1.74%` | `neutral` |
| `segment-index-hot-route-put:putHotRoute` | `2281110.336 ops/s` | `2542236.627 ops/s` | `+11.45%` | `better` |
| `segment-index-hot-route-put:putThenGetHotRoute` | `1529448.222 ops/s` | `1556262.634 ops/s` | `+1.75%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed` | `373376.383 ops/s` | `349406.448 ops/s` | `-6.42%` | `warning` |
| `segment-index-mixed-drain:partitionedIngestMixed:getWorkload` | `202914.379 ops/s` | `163814.558 ops/s` | `-19.27%` | `worse` |
| `segment-index-mixed-drain:partitionedIngestMixed:putWorkload` | `170462.004 ops/s` | `185591.890 ops/s` | `+8.88%` | `better` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed` | `26284.655 ops/s` | `36307.451 ops/s` | `+38.13%` | `better` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:getWorkload` | `20414.518 ops/s` | `29732.794 ops/s` | `+45.65%` | `better` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:putWorkload` | `5870.137 ops/s` | `6574.657 ops/s` | `+12.00%` | `better` |
| `segment-index-persisted-mutation:deleteSync` | `2762.872 ops/s` | `2699.392 ops/s` | `-2.30%` | `neutral` |
| `segment-index-persisted-mutation:putSync` | `2612.586 ops/s` | `2675.907 ops/s` | `+2.42%` | `neutral` |
