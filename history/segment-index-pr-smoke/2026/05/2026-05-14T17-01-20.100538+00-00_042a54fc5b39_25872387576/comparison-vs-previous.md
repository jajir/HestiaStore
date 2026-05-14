# Benchmark Comparison

- Profile: `segment-index-pr-smoke`
- Baseline SHA: `e942e650381bf00db7c2fbb3790ab0c49b708f39`
- Candidate SHA: `042a54fc5b39a821f487e959141a4dd5b63ff8e7`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `segment-index-get-live:getHitSync` | `2238195.122 ops/s` | `2386009.113 ops/s` | `+6.60%` | `better` |
| `segment-index-get-live:getMissSync` | `3909764.887 ops/s` | `3701690.459 ops/s` | `-5.32%` | `warning` |
| `segment-index-get-multisegment-hot:getHitSync` | `7193.851 ops/s` | `6967.323 ops/s` | `-3.15%` | `warning` |
| `segment-index-get-multisegment-hot:getMissSync` | `3890282.905 ops/s` | `3805888.531 ops/s` | `-2.17%` | `neutral` |
| `segment-index-get-persisted:getHitSync` | `1969820.536 ops/s` | `2123099.953 ops/s` | `+7.78%` | `better` |
| `segment-index-get-persisted:getMissSync` | `3844312.434 ops/s` | `3970527.138 ops/s` | `+3.28%` | `better` |
| `segment-index-hot-route-put:putHotRoute` | `1941175.071 ops/s` | `1907212.820 ops/s` | `-1.75%` | `neutral` |
| `segment-index-hot-route-put:putThenGetHotRoute` | `1073139.640 ops/s` | `1095329.893 ops/s` | `+2.07%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed` | `297271.143 ops/s` | `296407.036 ops/s` | `-0.29%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed:getWorkload` | `135917.461 ops/s` | `147911.616 ops/s` | `+8.82%` | `better` |
| `segment-index-mixed-drain:partitionedIngestMixed:putWorkload` | `161353.682 ops/s` | `148495.421 ops/s` | `-7.97%` | `worse` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed` | `38138.328 ops/s` | `41306.262 ops/s` | `+8.31%` | `better` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:getWorkload` | `32858.113 ops/s` | `36180.531 ops/s` | `+10.11%` | `better` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:putWorkload` | `5280.214 ops/s` | `5125.732 ops/s` | `-2.93%` | `neutral` |
