# Benchmark Comparison

- Profile: `segment-index-pr-smoke`
- Baseline SHA: `825d249ff4a901a6460ef32f4a00074eb47d28af`
- Candidate SHA: `0aaf66beeb09bb53c3a6668dd930c5175cc1bea9`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `segment-index-get-live:getHitSync` | `2204913.136 ops/s` | `2516845.724 ops/s` | `+14.15%` | `better` |
| `segment-index-get-live:getMissSync` | `2235238.467 ops/s` | `2234137.043 ops/s` | `-0.05%` | `neutral` |
| `segment-index-get-persisted:getHitSync` | `1929247.977 ops/s` | `1766049.229 ops/s` | `-8.46%` | `worse` |
| `segment-index-get-persisted:getMissSync` | `2057408.349 ops/s` | `2283039.703 ops/s` | `+10.97%` | `better` |
| `segment-index-hot-route-put:putHotRoute` | `2153720.037 ops/s` | `2151229.489 ops/s` | `-0.12%` | `neutral` |
| `segment-index-hot-route-put:putThenGetHotRoute` | `1084682.330 ops/s` | `1102827.777 ops/s` | `+1.67%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed` | `286739.113 ops/s` | `315604.325 ops/s` | `+10.07%` | `better` |
| `segment-index-mixed-drain:partitionedIngestMixed:getWorkload` | `136402.351 ops/s` | `159312.612 ops/s` | `+16.80%` | `better` |
| `segment-index-mixed-drain:partitionedIngestMixed:putWorkload` | `150336.761 ops/s` | `156291.712 ops/s` | `+3.96%` | `better` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed` | `204631.330 ops/s` | `202449.568 ops/s` | `-1.07%` | `neutral` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:getWorkload` | `190723.791 ops/s` | `188949.311 ops/s` | `-0.93%` | `neutral` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:putWorkload` | `13907.539 ops/s` | `13500.258 ops/s` | `-2.93%` | `neutral` |
