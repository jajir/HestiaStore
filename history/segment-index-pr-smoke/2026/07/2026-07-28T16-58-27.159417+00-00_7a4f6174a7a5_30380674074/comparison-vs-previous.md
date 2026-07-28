# Benchmark Comparison

- Profile: `segment-index-pr-smoke`
- Baseline SHA: `6802a1ecd057ef44daba17179d663834b289f16a`
- Candidate SHA: `7a4f6174a7a5f8a4815405ec895abbd159270a6e`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `segment-index-get-live:getHitSync` | `4799263.386 ops/s` | `5238568.216 ops/s` | `+9.15%` | `better` |
| `segment-index-get-live:getMissSync` | `4381866.561 ops/s` | `4588587.118 ops/s` | `+4.72%` | `better` |
| `segment-index-get-multisegment-hot:getHitSync` | `3280479.027 ops/s` | `3414808.084 ops/s` | `+4.09%` | `better` |
| `segment-index-get-multisegment-hot:getMissSync` | `4434571.463 ops/s` | `4818529.789 ops/s` | `+8.66%` | `better` |
| `segment-index-get-persisted:getHitSync` | `3437513.061 ops/s` | `3567568.282 ops/s` | `+3.78%` | `better` |
| `segment-index-get-persisted:getMissSync` | `4317225.295 ops/s` | `4837922.344 ops/s` | `+12.06%` | `better` |
| `segment-index-hot-route-put:putHotRoute` | `3980864.571 ops/s` | `4154777.745 ops/s` | `+4.37%` | `better` |
| `segment-index-hot-route-put:putThenGetHotRoute` | `2082054.860 ops/s` | `2115137.878 ops/s` | `+1.59%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed` | `583547.567 ops/s` | `566541.949 ops/s` | `-2.91%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed:getWorkload` | `414579.786 ops/s` | `400985.991 ops/s` | `-3.28%` | `warning` |
| `segment-index-mixed-drain:partitionedIngestMixed:putWorkload` | `168967.781 ops/s` | `165555.958 ops/s` | `-2.02%` | `neutral` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed` | `811158.630 ops/s` | `825501.570 ops/s` | `+1.77%` | `neutral` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:getWorkload` | `797465.345 ops/s` | `811902.355 ops/s` | `+1.81%` | `neutral` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:putWorkload` | `13693.285 ops/s` | `13599.216 ops/s` | `-0.69%` | `neutral` |
| `segment-index-persisted-mutation-concurrent:deleteSync` | `7241.097 ops/s` | `5721.307 ops/s` | `-20.99%` | `worse` |
| `segment-index-persisted-mutation-concurrent:putSync` | `7342.690 ops/s` | `5786.741 ops/s` | `-21.19%` | `worse` |
| `segment-index-persisted-mutation:deleteSync` | `3345.246 ops/s` | `2316.526 ops/s` | `-30.75%` | `worse` |
| `segment-index-persisted-mutation:putSync` | `3399.733 ops/s` | `2394.159 ops/s` | `-29.58%` | `worse` |
