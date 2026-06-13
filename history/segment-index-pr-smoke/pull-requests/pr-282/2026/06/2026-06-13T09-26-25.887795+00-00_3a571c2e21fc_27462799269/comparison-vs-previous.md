# Benchmark Comparison

- Profile: `segment-index-pr-smoke`
- Baseline SHA: `d60e9c3fc047d39eb0091b6dbadc07b8dc036ce7`
- Candidate SHA: `3a571c2e21fc0863df3dfb6d87ad8366fe1b11aa`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `segment-index-get-live:getHitSync` | `2411441.996 ops/s` | `2295176.406 ops/s` | `-4.82%` | `warning` |
| `segment-index-get-live:getMissSync` | `2094409.770 ops/s` | `2024134.592 ops/s` | `-3.36%` | `warning` |
| `segment-index-get-persisted:getHitSync` | `2013383.277 ops/s` | `2003501.739 ops/s` | `-0.49%` | `neutral` |
| `segment-index-get-persisted:getMissSync` | `2027317.261 ops/s` | `2144846.418 ops/s` | `+5.80%` | `better` |
| `segment-index-hot-route-put:putHotRoute` | `1925330.684 ops/s` | `2097925.099 ops/s` | `+8.96%` | `better` |
| `segment-index-hot-route-put:putThenGetHotRoute` | `1119924.874 ops/s` | `1158159.093 ops/s` | `+3.41%` | `better` |
| `segment-index-mixed-drain:partitionedIngestMixed` | `288815.066 ops/s` | `283211.300 ops/s` | `-1.94%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed:getWorkload` | `140704.219 ops/s` | `146999.214 ops/s` | `+4.47%` | `better` |
| `segment-index-mixed-drain:partitionedIngestMixed:putWorkload` | `148110.847 ops/s` | `136212.086 ops/s` | `-8.03%` | `worse` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed` | `193020.902 ops/s` | `178875.494 ops/s` | `-7.33%` | `worse` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:getWorkload` | `177998.085 ops/s` | `164932.678 ops/s` | `-7.34%` | `worse` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:putWorkload` | `15022.818 ops/s` | `13942.816 ops/s` | `-7.19%` | `worse` |
