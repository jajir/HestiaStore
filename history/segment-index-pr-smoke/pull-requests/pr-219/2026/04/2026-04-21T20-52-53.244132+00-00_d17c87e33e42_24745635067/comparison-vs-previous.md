# Benchmark Comparison

- Profile: `segment-index-pr-smoke`
- Baseline SHA: `9c725bd833aba4b408c00a1771c1bb051d786844`
- Candidate SHA: `d17c87e33e42663be8d0f9c711f887e34d857ec8`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `segment-index-get-live:getHitSync` | `2326175.347 ops/s` | `2056541.230 ops/s` | `-11.59%` | `worse` |
| `segment-index-get-live:getMissSync` | `3798421.688 ops/s` | `3764974.880 ops/s` | `-0.88%` | `neutral` |
| `segment-index-get-multisegment-hot:getHitSync` | `96.849 ops/s` | `75.122 ops/s` | `-22.43%` | `worse` |
| `segment-index-get-multisegment-hot:getMissSync` | `3745844.000 ops/s` | `3683458.417 ops/s` | `-1.67%` | `neutral` |
| `segment-index-get-persisted:getHitSync` | `113084.291 ops/s` | `117696.998 ops/s` | `+4.08%` | `better` |
| `segment-index-get-persisted:getMissSync` | `4063602.396 ops/s` | `3807455.421 ops/s` | `-6.30%` | `warning` |
| `segment-index-hot-route-put:putHotRoute` | `2083747.755 ops/s` | `2231593.703 ops/s` | `+7.10%` | `better` |
| `segment-index-hot-route-put:putThenGetHotRoute` | `1043000.625 ops/s` | `1130119.449 ops/s` | `+8.35%` | `better` |
| `segment-index-mixed-drain:partitionedIngestMixed` | `299919.486 ops/s` | `281405.216 ops/s` | `-6.17%` | `warning` |
| `segment-index-mixed-drain:partitionedIngestMixed:getWorkload` | `144468.190 ops/s` | `117302.516 ops/s` | `-18.80%` | `worse` |
| `segment-index-mixed-drain:partitionedIngestMixed:putWorkload` | `155451.297 ops/s` | `164102.700 ops/s` | `+5.57%` | `better` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed` | `112355.990 ops/s` | `6570.687 ops/s` | `-94.15%` | `worse` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:getWorkload` | `4501.155 ops/s` | `3248.753 ops/s` | `-27.82%` | `worse` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:putWorkload` | `107854.835 ops/s` | `3321.934 ops/s` | `-96.92%` | `worse` |
| `segment-index-persisted-mutation:deleteSync` | `2615.336 ops/s` | `2471.161 ops/s` | `-5.51%` | `warning` |
| `segment-index-persisted-mutation:putSync` | `2558.478 ops/s` | `2613.116 ops/s` | `+2.14%` | `neutral` |
