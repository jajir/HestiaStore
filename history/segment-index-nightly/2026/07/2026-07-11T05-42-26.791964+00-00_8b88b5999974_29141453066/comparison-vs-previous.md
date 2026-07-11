# Benchmark Comparison

- Profile: `segment-index-nightly`
- Baseline SHA: `1e37c5925410c7e6649b1fa1d4fd33325bcb2d4c`
- Candidate SHA: `8b88b599997497df9b956d4be577a2b7424cedcb`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `segment-index-get-live:getHitSync` | `1884935.849 ops/s` | `2095426.803 ops/s` | `+11.17%` | `better` |
| `segment-index-get-live:getMissSync` | `1947549.003 ops/s` | `2371388.277 ops/s` | `+21.76%` | `better` |
| `segment-index-get-persisted:getHitSync` | `1984662.033 ops/s` | `1839428.712 ops/s` | `-7.32%` | `worse` |
| `segment-index-get-persisted:getMissSync` | `1945984.723 ops/s` | `2138748.732 ops/s` | `+9.91%` | `better` |
| `segment-index-hot-route-put:putHotRoute` | `2159423.941 ops/s` | `2372635.895 ops/s` | `+9.87%` | `better` |
| `segment-index-hot-route-put:putThenGetHotRoute` | `1081040.921 ops/s` | `986914.321 ops/s` | `-8.71%` | `worse` |
| `segment-index-lifecycle:openAndCheckAndRepairConsistency` | `305.082 ms/op` | `309.342 ms/op` | `+1.40%` | `neutral` |
| `segment-index-lifecycle:openAndCompact` | `329.147 ms/op` | `325.739 ms/op` | `-1.04%` | `neutral` |
| `segment-index-lifecycle:openExisting` | `301.415 ms/op` | `301.074 ms/op` | `-0.11%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed` | `452957.900 ops/s` | `443180.815 ops/s` | `-2.16%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed:getWorkload` | `208503.804 ops/s` | `196348.356 ops/s` | `-5.83%` | `warning` |
| `segment-index-mixed-drain:partitionedIngestMixed:putWorkload` | `244454.096 ops/s` | `246832.459 ops/s` | `+0.97%` | `neutral` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed` | `881419.341 ops/s` | `906259.729 ops/s` | `+2.82%` | `neutral` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:getWorkload` | `862509.765 ops/s` | `888903.105 ops/s` | `+3.06%` | `better` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:putWorkload` | `18909.575 ops/s` | `17356.624 ops/s` | `-8.21%` | `worse` |
