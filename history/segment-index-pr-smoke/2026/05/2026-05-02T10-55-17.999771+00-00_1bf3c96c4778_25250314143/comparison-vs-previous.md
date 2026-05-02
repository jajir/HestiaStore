# Benchmark Comparison

- Profile: `segment-index-pr-smoke`
- Baseline SHA: `ce3de58c32e248c2050b7b04de33b021beecff74`
- Candidate SHA: `1bf3c96c4778c8cca6edc361e68f8c45fc3d9083`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `segment-index-get-live:getHitSync` | `2272152.352 ops/s` | `2369528.705 ops/s` | `+4.29%` | `better` |
| `segment-index-get-live:getMissSync` | `4097711.407 ops/s` | `3730179.464 ops/s` | `-8.97%` | `worse` |
| `segment-index-get-multisegment-hot:getHitSync` | `7069.352 ops/s` | `7132.769 ops/s` | `+0.90%` | `neutral` |
| `segment-index-get-multisegment-hot:getMissSync` | `3875870.149 ops/s` | `3726597.753 ops/s` | `-3.85%` | `warning` |
| `segment-index-get-persisted:getHitSync` | `113937.625 ops/s` | `119024.024 ops/s` | `+4.46%` | `better` |
| `segment-index-get-persisted:getMissSync` | `4048573.794 ops/s` | `4003546.285 ops/s` | `-1.11%` | `neutral` |
| `segment-index-hot-route-put:putHotRoute` | `2119487.265 ops/s` | `1920380.902 ops/s` | `-9.39%` | `worse` |
| `segment-index-hot-route-put:putThenGetHotRoute` | `1090235.815 ops/s` | `1087420.729 ops/s` | `-0.26%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed` | `285621.238 ops/s` | `289156.511 ops/s` | `+1.24%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed:getWorkload` | `112436.739 ops/s` | `124584.710 ops/s` | `+10.80%` | `better` |
| `segment-index-mixed-drain:partitionedIngestMixed:putWorkload` | `173184.499 ops/s` | `164571.801 ops/s` | `-4.97%` | `warning` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed` | `36996.691 ops/s` | `41398.166 ops/s` | `+11.90%` | `better` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:getWorkload` | `31878.016 ops/s` | `36190.796 ops/s` | `+13.53%` | `better` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:putWorkload` | `5118.674 ops/s` | `5207.370 ops/s` | `+1.73%` | `neutral` |
| `segment-index-persisted-mutation:deleteSync` | `2655.600 ops/s` | `2547.386 ops/s` | `-4.07%` | `warning` |
| `segment-index-persisted-mutation:putSync` | `449.084 ops/s` | `448.434 ops/s` | `-0.14%` | `neutral` |
