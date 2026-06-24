# Benchmark Comparison

- Profile: `segment-index-pr-smoke`
- Baseline SHA: `7cc96a4f02588fc1e87970fc84af8f7132a59154`
- Candidate SHA: `2289df12564ec66736c4db43808c46f5142fc088`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `segment-index-get-live:getHitSync` | `1756972.862 ops/s` | `1613836.248 ops/s` | `-8.15%` | `worse` |
| `segment-index-get-live:getMissSync` | `1847333.654 ops/s` | `1951031.044 ops/s` | `+5.61%` | `better` |
| `segment-index-get-persisted:getHitSync` | `1610134.763 ops/s` | `1678343.600 ops/s` | `+4.24%` | `better` |
| `segment-index-get-persisted:getMissSync` | `1736689.342 ops/s` | `1762334.990 ops/s` | `+1.48%` | `neutral` |
| `segment-index-hot-route-put:putHotRoute` | `1765689.450 ops/s` | `1761254.607 ops/s` | `-0.25%` | `neutral` |
| `segment-index-hot-route-put:putThenGetHotRoute` | `1037761.937 ops/s` | `1013296.504 ops/s` | `-2.36%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed` | `262037.219 ops/s` | `273209.548 ops/s` | `+4.26%` | `better` |
| `segment-index-mixed-drain:partitionedIngestMixed:getWorkload` | `99266.386 ops/s` | `131016.838 ops/s` | `+31.99%` | `better` |
| `segment-index-mixed-drain:partitionedIngestMixed:putWorkload` | `162770.833 ops/s` | `142192.710 ops/s` | `-12.64%` | `worse` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed` | `177296.689 ops/s` | `186275.619 ops/s` | `+5.06%` | `better` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:getWorkload` | `163444.465 ops/s` | `172670.018 ops/s` | `+5.64%` | `better` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:putWorkload` | `13852.223 ops/s` | `13605.600 ops/s` | `-1.78%` | `neutral` |
