# Benchmark Comparison

- Profile: `segment-index-pr-smoke`
- Baseline SHA: `3f1e4a8b99a603b0c0e0972d95816d2e32dbee76`
- Candidate SHA: `089a22c9f63c3d154ee6e80eb7f9384e62e15fe5`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `segment-index-get-live:getHitSync` | `2259412.111 ops/s` | `2258997.162 ops/s` | `-0.02%` | `neutral` |
| `segment-index-get-live:getMissSync` | `2176620.380 ops/s` | `2181666.500 ops/s` | `+0.23%` | `neutral` |
| `segment-index-get-persisted:getHitSync` | `1760680.628 ops/s` | `2107375.655 ops/s` | `+19.69%` | `better` |
| `segment-index-get-persisted:getMissSync` | `2110986.987 ops/s` | `2272058.965 ops/s` | `+7.63%` | `better` |
| `segment-index-hot-route-put:putHotRoute` | `2175196.262 ops/s` | `2115215.738 ops/s` | `-2.76%` | `neutral` |
| `segment-index-hot-route-put:putThenGetHotRoute` | `1038458.477 ops/s` | `1119915.613 ops/s` | `+7.84%` | `better` |
| `segment-index-mixed-drain:partitionedIngestMixed` | `333410.690 ops/s` | `315000.206 ops/s` | `-5.52%` | `warning` |
| `segment-index-mixed-drain:partitionedIngestMixed:getWorkload` | `172994.634 ops/s` | `165784.917 ops/s` | `-4.17%` | `warning` |
| `segment-index-mixed-drain:partitionedIngestMixed:putWorkload` | `160416.056 ops/s` | `149215.289 ops/s` | `-6.98%` | `warning` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed` | `189207.011 ops/s` | `207410.152 ops/s` | `+9.62%` | `better` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:getWorkload` | `175400.560 ops/s` | `193166.694 ops/s` | `+10.13%` | `better` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:putWorkload` | `13806.451 ops/s` | `14243.459 ops/s` | `+3.17%` | `better` |
