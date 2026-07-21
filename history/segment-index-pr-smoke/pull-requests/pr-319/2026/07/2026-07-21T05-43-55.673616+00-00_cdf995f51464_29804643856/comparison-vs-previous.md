# Benchmark Comparison

- Profile: `segment-index-pr-smoke`
- Baseline SHA: `73b0861c316726a3965ae86a737f820524e94f55`
- Candidate SHA: `cdf995f514644af1e8bf1c83744a0e26f9917040`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `segment-index-get-live:getHitSync` | `2390119.744 ops/s` | `2369716.830 ops/s` | `-0.85%` | `neutral` |
| `segment-index-get-live:getMissSync` | `1993777.753 ops/s` | `2093117.143 ops/s` | `+4.98%` | `better` |
| `segment-index-get-persisted:getHitSync` | `2033820.604 ops/s` | `2211857.086 ops/s` | `+8.75%` | `better` |
| `segment-index-get-persisted:getMissSync` | `2438654.069 ops/s` | `2078135.842 ops/s` | `-14.78%` | `worse` |
| `segment-index-hot-route-put:putHotRoute` | `2153855.822 ops/s` | `2237070.090 ops/s` | `+3.86%` | `better` |
| `segment-index-hot-route-put:putThenGetHotRoute` | `1080709.168 ops/s` | `1115205.043 ops/s` | `+3.19%` | `better` |
| `segment-index-mixed-drain:partitionedIngestMixed` | `470789.542 ops/s` | `479869.973 ops/s` | `+1.93%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed:getWorkload` | `325829.072 ops/s` | `339801.170 ops/s` | `+4.29%` | `better` |
| `segment-index-mixed-drain:partitionedIngestMixed:putWorkload` | `144960.470 ops/s` | `140068.803 ops/s` | `-3.37%` | `warning` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed` | `512449.079 ops/s` | `640568.675 ops/s` | `+25.00%` | `better` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:getWorkload` | `499301.187 ops/s` | `624420.861 ops/s` | `+25.06%` | `better` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:putWorkload` | `13147.892 ops/s` | `16147.814 ops/s` | `+22.82%` | `better` |
