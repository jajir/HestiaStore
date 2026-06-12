# Benchmark Comparison

- Profile: `segment-index-pr-smoke`
- Baseline SHA: `f2306ab30cfa91db7aaf1d581e69188e6b6049e5`
- Candidate SHA: `1756ecbfaee92319c50abf1110e55f35b5e37fd0`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `segment-index-get-live:getHitSync` | `2373343.947 ops/s` | `2488967.053 ops/s` | `+4.87%` | `better` |
| `segment-index-get-live:getMissSync` | `2135523.540 ops/s` | `2024189.980 ops/s` | `-5.21%` | `warning` |
| `segment-index-get-persisted:getHitSync` | `2100230.757 ops/s` | `1499647.854 ops/s` | `-28.60%` | `worse` |
| `segment-index-get-persisted:getMissSync` | `2031703.935 ops/s` | `1991338.451 ops/s` | `-1.99%` | `neutral` |
| `segment-index-hot-route-put:putHotRoute` | `2028444.924 ops/s` | `2095768.163 ops/s` | `+3.32%` | `better` |
| `segment-index-hot-route-put:putThenGetHotRoute` | `1155163.605 ops/s` | `1124837.226 ops/s` | `-2.63%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed` | `291498.929 ops/s` | `286188.785 ops/s` | `-1.82%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed:getWorkload` | `149396.151 ops/s` | `150095.437 ops/s` | `+0.47%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed:putWorkload` | `142102.778 ops/s` | `136093.348 ops/s` | `-4.23%` | `warning` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed` | `158039.136 ops/s` | `168597.471 ops/s` | `+6.68%` | `better` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:getWorkload` | `144176.821 ops/s` | `154174.584 ops/s` | `+6.93%` | `better` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:putWorkload` | `13862.314 ops/s` | `14422.886 ops/s` | `+4.04%` | `better` |
