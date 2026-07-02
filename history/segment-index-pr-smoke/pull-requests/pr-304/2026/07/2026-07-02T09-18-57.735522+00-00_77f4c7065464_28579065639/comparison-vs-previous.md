# Benchmark Comparison

- Profile: `segment-index-pr-smoke`
- Baseline SHA: `4d7ed0e894ecbc0e65a88805b11ec84b6b680e9c`
- Candidate SHA: `77f4c7065464d93ee38407def3ecac6975412b98`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `segment-index-get-live:getHitSync` | `2264685.317 ops/s` | `2315081.441 ops/s` | `+2.23%` | `neutral` |
| `segment-index-get-live:getMissSync` | `2265605.115 ops/s` | `2106670.678 ops/s` | `-7.02%` | `worse` |
| `segment-index-get-persisted:getHitSync` | `2098365.841 ops/s` | `2001377.579 ops/s` | `-4.62%` | `warning` |
| `segment-index-get-persisted:getMissSync` | `1996736.408 ops/s` | `2055124.955 ops/s` | `+2.92%` | `neutral` |
| `segment-index-hot-route-put:putHotRoute` | `2142479.140 ops/s` | `2177084.026 ops/s` | `+1.62%` | `neutral` |
| `segment-index-hot-route-put:putThenGetHotRoute` | `1114914.595 ops/s` | `1081748.269 ops/s` | `-2.97%` | `neutral` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed` | `609943.230 ops/s` | `-` | `-` | `removed` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:getWorkload` | `594628.958 ops/s` | `-` | `-` | `removed` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:putWorkload` | `15314.272 ops/s` | `-` | `-` | `removed` |
