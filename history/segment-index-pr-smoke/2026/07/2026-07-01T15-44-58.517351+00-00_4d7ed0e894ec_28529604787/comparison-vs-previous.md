# Benchmark Comparison

- Profile: `segment-index-pr-smoke`
- Baseline SHA: `67bbf8e34d2b602923270722df932c97760ccab3`
- Candidate SHA: `4d7ed0e894ecbc0e65a88805b11ec84b6b680e9c`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `segment-index-get-live:getHitSync` | `2406565.483 ops/s` | `2301715.159 ops/s` | `-4.36%` | `warning` |
| `segment-index-get-live:getMissSync` | `1985954.474 ops/s` | `2291201.056 ops/s` | `+15.37%` | `better` |
| `segment-index-get-persisted:getHitSync` | `1689157.497 ops/s` | `2014873.074 ops/s` | `+19.28%` | `better` |
| `segment-index-get-persisted:getMissSync` | `2113058.282 ops/s` | `2242942.198 ops/s` | `+6.15%` | `better` |
| `segment-index-hot-route-put:putHotRoute` | `2160413.013 ops/s` | `2073613.401 ops/s` | `-4.02%` | `warning` |
| `segment-index-hot-route-put:putThenGetHotRoute` | `1104102.892 ops/s` | `1128626.366 ops/s` | `+2.22%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed` | `424751.380 ops/s` | `-` | `-` | `removed` |
| `segment-index-mixed-drain:partitionedIngestMixed:getWorkload` | `263428.272 ops/s` | `-` | `-` | `removed` |
| `segment-index-mixed-drain:partitionedIngestMixed:putWorkload` | `161323.108 ops/s` | `-` | `-` | `removed` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed` | `512773.939 ops/s` | `-` | `-` | `removed` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:getWorkload` | `499919.495 ops/s` | `-` | `-` | `removed` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:putWorkload` | `12854.444 ops/s` | `-` | `-` | `removed` |
