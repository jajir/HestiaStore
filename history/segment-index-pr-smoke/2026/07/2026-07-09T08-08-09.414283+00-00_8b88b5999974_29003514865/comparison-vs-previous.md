# Benchmark Comparison

- Profile: `segment-index-pr-smoke`
- Baseline SHA: `1e37c5925410c7e6649b1fa1d4fd33325bcb2d4c`
- Candidate SHA: `8b88b599997497df9b956d4be577a2b7424cedcb`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `segment-index-get-live:getHitSync` | `2252740.759 ops/s` | `2212416.830 ops/s` | `-1.79%` | `neutral` |
| `segment-index-get-live:getMissSync` | `2191104.995 ops/s` | `2195169.974 ops/s` | `+0.19%` | `neutral` |
| `segment-index-get-persisted:getHitSync` | `1898937.091 ops/s` | `1880367.136 ops/s` | `-0.98%` | `neutral` |
| `segment-index-get-persisted:getMissSync` | `1885411.552 ops/s` | `2351774.540 ops/s` | `+24.74%` | `better` |
| `segment-index-hot-route-put:putHotRoute` | `1971582.236 ops/s` | `2153749.233 ops/s` | `+9.24%` | `better` |
| `segment-index-hot-route-put:putThenGetHotRoute` | `1099662.880 ops/s` | `1116492.199 ops/s` | `+1.53%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed` | `484646.146 ops/s` | `416937.514 ops/s` | `-13.97%` | `worse` |
| `segment-index-mixed-drain:partitionedIngestMixed:getWorkload` | `321807.847 ops/s` | `245342.124 ops/s` | `-23.76%` | `worse` |
| `segment-index-mixed-drain:partitionedIngestMixed:putWorkload` | `162838.299 ops/s` | `171595.390 ops/s` | `+5.38%` | `better` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed` | `515781.838 ops/s` | `660933.403 ops/s` | `+28.14%` | `better` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:getWorkload` | `502794.652 ops/s` | `645211.223 ops/s` | `+28.32%` | `better` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:putWorkload` | `12987.187 ops/s` | `15722.179 ops/s` | `+21.06%` | `better` |
