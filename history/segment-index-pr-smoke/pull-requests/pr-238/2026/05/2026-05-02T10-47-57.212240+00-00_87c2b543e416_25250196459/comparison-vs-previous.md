# Benchmark Comparison

- Profile: `segment-index-pr-smoke`
- Baseline SHA: `ce3de58c32e248c2050b7b04de33b021beecff74`
- Candidate SHA: `87c2b543e416892960807b82a4f9dcab598b2b59`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `segment-index-get-live:getHitSync` | `2272152.352 ops/s` | `2391159.900 ops/s` | `+5.24%` | `better` |
| `segment-index-get-live:getMissSync` | `4097711.407 ops/s` | `3746414.546 ops/s` | `-8.57%` | `worse` |
| `segment-index-get-multisegment-hot:getHitSync` | `7069.352 ops/s` | `7878.119 ops/s` | `+11.44%` | `better` |
| `segment-index-get-multisegment-hot:getMissSync` | `3875870.149 ops/s` | `3886267.717 ops/s` | `+0.27%` | `neutral` |
| `segment-index-get-persisted:getHitSync` | `113937.625 ops/s` | `113022.143 ops/s` | `-0.80%` | `neutral` |
| `segment-index-get-persisted:getMissSync` | `4048573.794 ops/s` | `4012105.401 ops/s` | `-0.90%` | `neutral` |
| `segment-index-hot-route-put:putHotRoute` | `2119487.265 ops/s` | `1942001.937 ops/s` | `-8.37%` | `worse` |
| `segment-index-hot-route-put:putThenGetHotRoute` | `1090235.815 ops/s` | `1084589.926 ops/s` | `-0.52%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed` | `285621.238 ops/s` | `288966.842 ops/s` | `+1.17%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed:getWorkload` | `112436.739 ops/s` | `126616.352 ops/s` | `+12.61%` | `better` |
| `segment-index-mixed-drain:partitionedIngestMixed:putWorkload` | `173184.499 ops/s` | `162350.491 ops/s` | `-6.26%` | `warning` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed` | `36996.691 ops/s` | `39590.040 ops/s` | `+7.01%` | `better` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:getWorkload` | `31878.016 ops/s` | `34297.347 ops/s` | `+7.59%` | `better` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:putWorkload` | `5118.674 ops/s` | `5292.693 ops/s` | `+3.40%` | `better` |
| `segment-index-persisted-mutation:deleteSync` | `2655.600 ops/s` | `2612.398 ops/s` | `-1.63%` | `neutral` |
| `segment-index-persisted-mutation:putSync` | `449.084 ops/s` | `449.368 ops/s` | `+0.06%` | `neutral` |
