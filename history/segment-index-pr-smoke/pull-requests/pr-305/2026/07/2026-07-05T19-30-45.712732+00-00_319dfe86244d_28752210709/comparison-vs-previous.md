# Benchmark Comparison

- Profile: `segment-index-pr-smoke`
- Baseline SHA: `027ee122b625a6c006b7d04ba7284fb8a4e6d9e8`
- Candidate SHA: `319dfe86244df661bd7b4931e0bfb2a847c7877c`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `segment-index-get-live:getHitSync` | `2139147.493 ops/s` | `2276183.565 ops/s` | `+6.41%` | `better` |
| `segment-index-get-live:getMissSync` | `2288668.953 ops/s` | `2101499.549 ops/s` | `-8.18%` | `worse` |
| `segment-index-get-persisted:getHitSync` | `1730654.919 ops/s` | `1985275.157 ops/s` | `+14.71%` | `better` |
| `segment-index-get-persisted:getMissSync` | `1968771.207 ops/s` | `2150714.803 ops/s` | `+9.24%` | `better` |
| `segment-index-hot-route-put:putHotRoute` | `2148244.050 ops/s` | `2159277.795 ops/s` | `+0.51%` | `neutral` |
| `segment-index-hot-route-put:putThenGetHotRoute` | `1138923.995 ops/s` | `1102678.865 ops/s` | `-3.18%` | `warning` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed` | `622164.109 ops/s` | `685440.099 ops/s` | `+10.17%` | `better` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:getWorkload` | `608457.715 ops/s` | `670163.034 ops/s` | `+10.14%` | `better` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:putWorkload` | `13706.394 ops/s` | `15277.065 ops/s` | `+11.46%` | `better` |
