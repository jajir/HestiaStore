# Benchmark Comparison

- Profile: `segment-index-pr-smoke`
- Baseline SHA: `efe7e934705b93964a14c80634f0a9e989e6768c`
- Candidate SHA: `240bde3d9176c1a1ab1fcd7a76c939364cdded57`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `segment-index-get-live:getHitSync` | `2141884.027 ops/s` | `2338237.705 ops/s` | `+9.17%` | `better` |
| `segment-index-get-live:getMissSync` | `1950379.719 ops/s` | `2210164.450 ops/s` | `+13.32%` | `better` |
| `segment-index-get-persisted:getHitSync` | `1990241.589 ops/s` | `1708003.162 ops/s` | `-14.18%` | `worse` |
| `segment-index-get-persisted:getMissSync` | `2080937.773 ops/s` | `2007503.481 ops/s` | `-3.53%` | `warning` |
| `segment-index-hot-route-put:putHotRoute` | `2001202.255 ops/s` | `1950243.636 ops/s` | `-2.55%` | `neutral` |
| `segment-index-hot-route-put:putThenGetHotRoute` | `1064654.194 ops/s` | `1033736.881 ops/s` | `-2.90%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed` | `300702.292 ops/s` | `303579.565 ops/s` | `+0.96%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed:getWorkload` | `141106.321 ops/s` | `145164.366 ops/s` | `+2.88%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed:putWorkload` | `159595.971 ops/s` | `158415.200 ops/s` | `-0.74%` | `neutral` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed` | `186324.986 ops/s` | `191535.729 ops/s` | `+2.80%` | `neutral` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:getWorkload` | `172979.934 ops/s` | `177311.757 ops/s` | `+2.50%` | `neutral` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:putWorkload` | `13345.052 ops/s` | `14223.972 ops/s` | `+6.59%` | `better` |
