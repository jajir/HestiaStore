# Benchmark Comparison

- Profile: `segment-index-pr-smoke`
- Baseline SHA: `0cd6b4f3d2afb20ac2e4c8576adc66a7b6319770`
- Candidate SHA: `73b0861c316726a3965ae86a737f820524e94f55`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `segment-index-get-live:getHitSync` | `2363532.353 ops/s` | `2376770.849 ops/s` | `+0.56%` | `neutral` |
| `segment-index-get-live:getMissSync` | `2025514.378 ops/s` | `2209048.118 ops/s` | `+9.06%` | `better` |
| `segment-index-get-persisted:getHitSync` | `2017052.245 ops/s` | `2165441.520 ops/s` | `+7.36%` | `better` |
| `segment-index-get-persisted:getMissSync` | `2229819.968 ops/s` | `2094737.154 ops/s` | `-6.06%` | `warning` |
| `segment-index-hot-route-put:putHotRoute` | `2116309.389 ops/s` | `2171685.950 ops/s` | `+2.62%` | `neutral` |
| `segment-index-hot-route-put:putThenGetHotRoute` | `1093493.415 ops/s` | `1121742.459 ops/s` | `+2.58%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed` | `464532.323 ops/s` | `454566.559 ops/s` | `-2.15%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed:getWorkload` | `301707.925 ops/s` | `295909.637 ops/s` | `-1.92%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed:putWorkload` | `162824.398 ops/s` | `158656.922 ops/s` | `-2.56%` | `neutral` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed` | `595099.417 ops/s` | `513705.488 ops/s` | `-13.68%` | `worse` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:getWorkload` | `580518.519 ops/s` | `500383.363 ops/s` | `-13.80%` | `worse` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:putWorkload` | `14580.898 ops/s` | `13322.124 ops/s` | `-8.63%` | `worse` |
