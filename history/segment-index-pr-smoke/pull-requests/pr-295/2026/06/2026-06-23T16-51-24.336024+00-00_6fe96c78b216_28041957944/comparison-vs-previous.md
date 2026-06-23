# Benchmark Comparison

- Profile: `segment-index-pr-smoke`
- Baseline SHA: `8402d6728c8b3d0957fe89ad3908037071cf38b2`
- Candidate SHA: `6fe96c78b216c3a580c2e6feb176c4baf9401b33`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `segment-index-get-live:getHitSync` | `2180383.567 ops/s` | `2303588.321 ops/s` | `+5.65%` | `better` |
| `segment-index-get-live:getMissSync` | `1996038.928 ops/s` | `1887657.541 ops/s` | `-5.43%` | `warning` |
| `segment-index-get-persisted:getHitSync` | `1908813.498 ops/s` | `1801161.468 ops/s` | `-5.64%` | `warning` |
| `segment-index-get-persisted:getMissSync` | `2223571.836 ops/s` | `1912200.369 ops/s` | `-14.00%` | `worse` |
| `segment-index-hot-route-put:putHotRoute` | `2002994.898 ops/s` | `1971334.625 ops/s` | `-1.58%` | `neutral` |
| `segment-index-hot-route-put:putThenGetHotRoute` | `1100838.142 ops/s` | `1046722.658 ops/s` | `-4.92%` | `warning` |
| `segment-index-mixed-drain:partitionedIngestMixed` | `285785.746 ops/s` | `308998.124 ops/s` | `+8.12%` | `better` |
| `segment-index-mixed-drain:partitionedIngestMixed:getWorkload` | `135817.947 ops/s` | `167098.468 ops/s` | `+23.03%` | `better` |
| `segment-index-mixed-drain:partitionedIngestMixed:putWorkload` | `149967.799 ops/s` | `141899.656 ops/s` | `-5.38%` | `warning` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed` | `172393.452 ops/s` | `174730.020 ops/s` | `+1.36%` | `neutral` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:getWorkload` | `158926.911 ops/s` | `160697.054 ops/s` | `+1.11%` | `neutral` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:putWorkload` | `13466.540 ops/s` | `14032.965 ops/s` | `+4.21%` | `better` |
