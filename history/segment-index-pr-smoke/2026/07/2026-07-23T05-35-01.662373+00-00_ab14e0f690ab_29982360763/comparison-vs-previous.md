# Benchmark Comparison

- Profile: `segment-index-pr-smoke`
- Baseline SHA: `f77d9f36b6a44cdd36ce858c25c4df8e677009b9`
- Candidate SHA: `ab14e0f690ab3a6317b666554a234ec2ec604446`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `segment-index-get-live:getHitSync` | `4681968.226 ops/s` | `4625588.367 ops/s` | `-1.20%` | `neutral` |
| `segment-index-get-live:getMissSync` | `4247848.828 ops/s` | `4334922.897 ops/s` | `+2.05%` | `neutral` |
| `segment-index-get-persisted:getHitSync` | `3443576.164 ops/s` | `3300409.689 ops/s` | `-4.16%` | `warning` |
| `segment-index-get-persisted:getMissSync` | `4072577.155 ops/s` | `4325814.046 ops/s` | `+6.22%` | `better` |
| `segment-index-hot-route-put:putHotRoute` | `3539344.841 ops/s` | `3660427.557 ops/s` | `+3.42%` | `better` |
| `segment-index-hot-route-put:putThenGetHotRoute` | `1960040.957 ops/s` | `1807138.917 ops/s` | `-7.80%` | `worse` |
| `segment-index-mixed-drain:partitionedIngestMixed` | `547951.676 ops/s` | `584614.711 ops/s` | `+6.69%` | `better` |
| `segment-index-mixed-drain:partitionedIngestMixed:getWorkload` | `381855.850 ops/s` | `405090.634 ops/s` | `+6.08%` | `better` |
| `segment-index-mixed-drain:partitionedIngestMixed:putWorkload` | `166095.826 ops/s` | `179524.077 ops/s` | `+8.08%` | `better` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed` | `655651.119 ops/s` | `613055.459 ops/s` | `-6.50%` | `warning` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:getWorkload` | `643056.950 ops/s` | `599739.694 ops/s` | `-6.74%` | `warning` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:putWorkload` | `12594.169 ops/s` | `13315.764 ops/s` | `+5.73%` | `better` |
| `segment-index-persisted-mutation:deleteSync` | `3484.339 ops/s` | `3375.488 ops/s` | `-3.12%` | `warning` |
| `segment-index-persisted-mutation:putSync` | `3551.192 ops/s` | `3413.437 ops/s` | `-3.88%` | `warning` |
