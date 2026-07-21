# Benchmark Comparison

- Profile: `segment-index-nightly`
- Baseline SHA: `21d1da156bc01e2f39b9f119c5882d79b3da438a`
- Candidate SHA: `dce1fbc911dbad09d8df685697a7c8b43069ed09`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `segment-index-get-live:getHitSync` | `2661074.501 ops/s` | `2087001.513 ops/s` | `-21.57%` | `worse` |
| `segment-index-get-live:getMissSync` | `1739592.346 ops/s` | `1890602.140 ops/s` | `+8.68%` | `better` |
| `segment-index-get-persisted:getHitSync` | `1805220.376 ops/s` | `1607037.645 ops/s` | `-10.98%` | `worse` |
| `segment-index-get-persisted:getMissSync` | `1865892.513 ops/s` | `2077830.509 ops/s` | `+11.36%` | `better` |
| `segment-index-hot-route-put:putHotRoute` | `2039601.686 ops/s` | `2191321.834 ops/s` | `+7.44%` | `better` |
| `segment-index-hot-route-put:putThenGetHotRoute` | `1067216.090 ops/s` | `1069217.450 ops/s` | `+0.19%` | `neutral` |
| `segment-index-lifecycle:openAndCheckAndRepairConsistency` | `259.656 ms/op` | `261.039 ms/op` | `+0.53%` | `neutral` |
| `segment-index-lifecycle:openAndCompact` | `281.213 ms/op` | `280.271 ms/op` | `-0.34%` | `neutral` |
| `segment-index-lifecycle:openExisting` | `257.160 ms/op` | `258.316 ms/op` | `+0.45%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed` | `430945.267 ops/s` | `434739.993 ops/s` | `+0.88%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed:getWorkload` | `201511.446 ops/s` | `196932.055 ops/s` | `-2.27%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed:putWorkload` | `229433.821 ops/s` | `237807.939 ops/s` | `+3.65%` | `better` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed` | `902009.957 ops/s` | `872873.497 ops/s` | `-3.23%` | `warning` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:getWorkload` | `883805.248 ops/s` | `856032.325 ops/s` | `-3.14%` | `warning` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:putWorkload` | `18204.709 ops/s` | `16841.172 ops/s` | `-7.49%` | `worse` |
