# Benchmark Comparison

- Profile: `segment-index-nightly`
- Baseline SHA: `1e37c5925410c7e6649b1fa1d4fd33325bcb2d4c`
- Candidate SHA: `8b88b599997497df9b956d4be577a2b7424cedcb`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `segment-index-get-live:getHitSync` | `2223615.152 ops/s` | `1971795.453 ops/s` | `-11.32%` | `worse` |
| `segment-index-get-live:getMissSync` | `2045648.907 ops/s` | `1933369.138 ops/s` | `-5.49%` | `warning` |
| `segment-index-get-multisegment-hot:getMissSync` | `-` | `1803091.617 ops/s` | `-` | `new` |
| `segment-index-get-persisted:getHitSync` | `1577854.271 ops/s` | `1740238.613 ops/s` | `+10.29%` | `better` |
| `segment-index-get-persisted:getMissSync` | `1949850.648 ops/s` | `1964741.260 ops/s` | `+0.76%` | `neutral` |
| `segment-index-hot-route-put:putHotRoute` | `2222172.725 ops/s` | `1938034.984 ops/s` | `-12.79%` | `worse` |
| `segment-index-hot-route-put:putThenGetHotRoute` | `969675.665 ops/s` | `1119329.538 ops/s` | `+15.43%` | `better` |
| `segment-index-lifecycle:openAndCheckAndRepairConsistency` | `245.257 ms/op` | `246.542 ms/op` | `+0.52%` | `neutral` |
| `segment-index-lifecycle:openAndCompact` | `267.323 ms/op` | `268.814 ms/op` | `+0.56%` | `neutral` |
| `segment-index-lifecycle:openExisting` | `245.479 ms/op` | `243.378 ms/op` | `-0.86%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed` | `431429.445 ops/s` | `447654.143 ops/s` | `+3.76%` | `better` |
| `segment-index-mixed-drain:partitionedIngestMixed:getWorkload` | `193026.764 ops/s` | `214799.998 ops/s` | `+11.28%` | `better` |
| `segment-index-mixed-drain:partitionedIngestMixed:putWorkload` | `238402.681 ops/s` | `232854.146 ops/s` | `-2.33%` | `neutral` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed` | `859317.712 ops/s` | `887838.944 ops/s` | `+3.32%` | `better` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:getWorkload` | `842859.420 ops/s` | `869923.717 ops/s` | `+3.21%` | `better` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:putWorkload` | `16458.291 ops/s` | `17915.227 ops/s` | `+8.85%` | `better` |
