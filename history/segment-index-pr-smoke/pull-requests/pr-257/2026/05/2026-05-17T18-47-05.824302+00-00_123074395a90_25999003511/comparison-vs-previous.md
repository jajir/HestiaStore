# Benchmark Comparison

- Profile: `segment-index-pr-smoke`
- Baseline SHA: `a232ff96e55757742a257a30e43a12f29ad641bd`
- Candidate SHA: `123074395a9099525e57390c588a06c28c317757`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `segment-index-get-live:getHitSync` | `2438461.945 ops/s` | `2233446.716 ops/s` | `-8.41%` | `worse` |
| `segment-index-get-live:getMissSync` | `3971551.874 ops/s` | `3999280.709 ops/s` | `+0.70%` | `neutral` |
| `segment-index-get-multisegment-hot:getHitSync` | `7242.835 ops/s` | `13741.618 ops/s` | `+89.73%` | `better` |
| `segment-index-get-multisegment-hot:getMissSync` | `4020472.953 ops/s` | `3812450.102 ops/s` | `-5.17%` | `warning` |
| `segment-index-get-persisted:getHitSync` | `1972996.980 ops/s` | `1753684.880 ops/s` | `-11.12%` | `worse` |
| `segment-index-get-persisted:getMissSync` | `4130342.157 ops/s` | `3978343.974 ops/s` | `-3.68%` | `warning` |
| `segment-index-hot-route-put:putHotRoute` | `1901736.548 ops/s` | `1687038.505 ops/s` | `-11.29%` | `worse` |
| `segment-index-hot-route-put:putThenGetHotRoute` | `1035822.068 ops/s` | `1042933.175 ops/s` | `+0.69%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed` | `286054.150 ops/s` | `262382.671 ops/s` | `-8.28%` | `worse` |
| `segment-index-mixed-drain:partitionedIngestMixed:getWorkload` | `115312.116 ops/s` | `112055.773 ops/s` | `-2.82%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed:putWorkload` | `170742.034 ops/s` | `150326.897 ops/s` | `-11.96%` | `worse` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed` | `42640.155 ops/s` | `46485.630 ops/s` | `+9.02%` | `better` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:getWorkload` | `37362.473 ops/s` | `41148.364 ops/s` | `+10.13%` | `better` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:putWorkload` | `5277.683 ops/s` | `5337.266 ops/s` | `+1.13%` | `neutral` |
