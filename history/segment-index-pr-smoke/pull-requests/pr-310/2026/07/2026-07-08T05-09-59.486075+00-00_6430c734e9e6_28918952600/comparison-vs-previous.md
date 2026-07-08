# Benchmark Comparison

- Profile: `segment-index-pr-smoke`
- Baseline SHA: `6309468d3d7f8677a76ad087d9c50bde66b3f144`
- Candidate SHA: `6430c734e9e6465f56d052a56b2da3113d4ab8f7`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `segment-index-get-live:getHitSync` | `2135578.916 ops/s` | `2240694.343 ops/s` | `+4.92%` | `better` |
| `segment-index-get-live:getMissSync` | `2133258.899 ops/s` | `2268475.936 ops/s` | `+6.34%` | `better` |
| `segment-index-get-persisted:getHitSync` | `1750562.607 ops/s` | `2070618.261 ops/s` | `+18.28%` | `better` |
| `segment-index-get-persisted:getMissSync` | `2248668.377 ops/s` | `2135116.624 ops/s` | `-5.05%` | `warning` |
| `segment-index-hot-route-put:putHotRoute` | `2152984.949 ops/s` | `2087109.513 ops/s` | `-3.06%` | `warning` |
| `segment-index-hot-route-put:putThenGetHotRoute` | `1157458.395 ops/s` | `1082548.863 ops/s` | `-6.47%` | `warning` |
| `segment-index-mixed-drain:partitionedIngestMixed` | `459428.877 ops/s` | `441020.975 ops/s` | `-4.01%` | `warning` |
| `segment-index-mixed-drain:partitionedIngestMixed:getWorkload` | `294527.904 ops/s` | `269028.933 ops/s` | `-8.66%` | `worse` |
| `segment-index-mixed-drain:partitionedIngestMixed:putWorkload` | `164900.973 ops/s` | `171992.042 ops/s` | `+4.30%` | `better` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed` | `682160.560 ops/s` | `558913.474 ops/s` | `-18.07%` | `worse` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:getWorkload` | `666467.804 ops/s` | `545461.995 ops/s` | `-18.16%` | `worse` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:putWorkload` | `15692.757 ops/s` | `13451.479 ops/s` | `-14.28%` | `worse` |
