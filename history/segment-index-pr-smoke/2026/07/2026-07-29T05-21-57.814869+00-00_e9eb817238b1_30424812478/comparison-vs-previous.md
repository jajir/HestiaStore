# Benchmark Comparison

- Profile: `segment-index-pr-smoke`
- Baseline SHA: `7a4f6174a7a5f8a4815405ec895abbd159270a6e`
- Candidate SHA: `e9eb817238b13a28ea6eb176dc563428e244553c`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `segment-index-get-live:getHitSync` | `5238568.216 ops/s` | `5082264.755 ops/s` | `-2.98%` | `neutral` |
| `segment-index-get-live:getMissSync` | `4588587.118 ops/s` | `4537420.599 ops/s` | `-1.12%` | `neutral` |
| `segment-index-get-multisegment-hot:getHitSync` | `3414808.084 ops/s` | `3676927.091 ops/s` | `+7.68%` | `better` |
| `segment-index-get-multisegment-hot:getMissSync` | `4818529.789 ops/s` | `4707989.660 ops/s` | `-2.29%` | `neutral` |
| `segment-index-get-persisted:getHitSync` | `3567568.282 ops/s` | `3466963.874 ops/s` | `-2.82%` | `neutral` |
| `segment-index-get-persisted:getMissSync` | `4837922.344 ops/s` | `4704501.607 ops/s` | `-2.76%` | `neutral` |
| `segment-index-hot-route-put:putHotRoute` | `4154777.745 ops/s` | `4163428.460 ops/s` | `+0.21%` | `neutral` |
| `segment-index-hot-route-put:putThenGetHotRoute` | `2115137.878 ops/s` | `2187331.166 ops/s` | `+3.41%` | `better` |
| `segment-index-mixed-drain:partitionedIngestMixed` | `566541.949 ops/s` | `566117.213 ops/s` | `-0.07%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed:getWorkload` | `400985.991 ops/s` | `414630.623 ops/s` | `+3.40%` | `better` |
| `segment-index-mixed-drain:partitionedIngestMixed:putWorkload` | `165555.958 ops/s` | `151486.590 ops/s` | `-8.50%` | `worse` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed` | `825501.570 ops/s` | `775805.467 ops/s` | `-6.02%` | `warning` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:getWorkload` | `811902.355 ops/s` | `762499.383 ops/s` | `-6.08%` | `warning` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:putWorkload` | `13599.216 ops/s` | `13306.084 ops/s` | `-2.16%` | `neutral` |
| `segment-index-persisted-mutation-concurrent:deleteSync` | `5721.307 ops/s` | `6438.832 ops/s` | `+12.54%` | `better` |
| `segment-index-persisted-mutation-concurrent:putSync` | `5786.741 ops/s` | `6456.269 ops/s` | `+11.57%` | `better` |
| `segment-index-persisted-mutation:deleteSync` | `2316.526 ops/s` | `2657.525 ops/s` | `+14.72%` | `better` |
| `segment-index-persisted-mutation:putSync` | `2394.159 ops/s` | `2218.228 ops/s` | `-7.35%` | `worse` |
