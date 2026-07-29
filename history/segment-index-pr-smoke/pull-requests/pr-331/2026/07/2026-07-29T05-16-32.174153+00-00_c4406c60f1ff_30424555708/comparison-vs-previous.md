# Benchmark Comparison

- Profile: `segment-index-pr-smoke`
- Baseline SHA: `7a4f6174a7a5f8a4815405ec895abbd159270a6e`
- Candidate SHA: `c4406c60f1ff6a82164f52b90ab692647b4f4a2b`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `segment-index-get-live:getHitSync` | `5238568.216 ops/s` | `4931318.038 ops/s` | `-5.87%` | `warning` |
| `segment-index-get-live:getMissSync` | `4588587.118 ops/s` | `4342924.114 ops/s` | `-5.35%` | `warning` |
| `segment-index-get-multisegment-hot:getHitSync` | `3414808.084 ops/s` | `3290066.059 ops/s` | `-3.65%` | `warning` |
| `segment-index-get-multisegment-hot:getMissSync` | `4818529.789 ops/s` | `4340025.015 ops/s` | `-9.93%` | `worse` |
| `segment-index-get-persisted:getHitSync` | `3567568.282 ops/s` | `3278599.195 ops/s` | `-8.10%` | `worse` |
| `segment-index-get-persisted:getMissSync` | `4837922.344 ops/s` | `4449075.761 ops/s` | `-8.04%` | `worse` |
| `segment-index-hot-route-put:putHotRoute` | `4154777.745 ops/s` | `3696559.886 ops/s` | `-11.03%` | `worse` |
| `segment-index-hot-route-put:putThenGetHotRoute` | `2115137.878 ops/s` | `2119998.520 ops/s` | `+0.23%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed` | `566541.949 ops/s` | `628827.886 ops/s` | `+10.99%` | `better` |
| `segment-index-mixed-drain:partitionedIngestMixed:getWorkload` | `400985.991 ops/s` | `460063.748 ops/s` | `+14.73%` | `better` |
| `segment-index-mixed-drain:partitionedIngestMixed:putWorkload` | `165555.958 ops/s` | `168764.138 ops/s` | `+1.94%` | `neutral` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed` | `825501.570 ops/s` | `917013.219 ops/s` | `+11.09%` | `better` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:getWorkload` | `811902.355 ops/s` | `903053.667 ops/s` | `+11.23%` | `better` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:putWorkload` | `13599.216 ops/s` | `13959.552 ops/s` | `+2.65%` | `neutral` |
| `segment-index-persisted-mutation-concurrent:deleteSync` | `5721.307 ops/s` | `7219.912 ops/s` | `+26.19%` | `better` |
| `segment-index-persisted-mutation-concurrent:putSync` | `5786.741 ops/s` | `6825.643 ops/s` | `+17.95%` | `better` |
| `segment-index-persisted-mutation:deleteSync` | `2316.526 ops/s` | `3358.378 ops/s` | `+44.97%` | `better` |
| `segment-index-persisted-mutation:putSync` | `2394.159 ops/s` | `3329.109 ops/s` | `+39.05%` | `better` |
