# Benchmark Comparison

- Profile: `segment-index-pr-smoke`
- Baseline SHA: `ba315c9b6d99e9ef021ee4ca3f5ac8b78827fd14`
- Candidate SHA: `55de924ccf3e8be908be6dfe7fc2dd8d88d7d938`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `segment-index-get-live:getHitSync` | `2367796.691 ops/s` | `2197543.424 ops/s` | `-7.19%` | `worse` |
| `segment-index-get-live:getMissSync` | `1946862.970 ops/s` | `2207177.625 ops/s` | `+13.37%` | `better` |
| `segment-index-get-persisted:getHitSync` | `2076333.914 ops/s` | `1794387.861 ops/s` | `-13.58%` | `worse` |
| `segment-index-get-persisted:getMissSync` | `2111714.077 ops/s` | `2161830.224 ops/s` | `+2.37%` | `neutral` |
| `segment-index-hot-route-put:putHotRoute` | `2169411.967 ops/s` | `2168827.650 ops/s` | `-0.03%` | `neutral` |
| `segment-index-hot-route-put:putThenGetHotRoute` | `1063214.369 ops/s` | `1086459.717 ops/s` | `+2.19%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed` | `467144.074 ops/s` | `440945.110 ops/s` | `-5.61%` | `warning` |
| `segment-index-mixed-drain:partitionedIngestMixed:getWorkload` | `311952.464 ops/s` | `271703.263 ops/s` | `-12.90%` | `worse` |
| `segment-index-mixed-drain:partitionedIngestMixed:putWorkload` | `155191.609 ops/s` | `169241.847 ops/s` | `+9.05%` | `better` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed` | `578162.679 ops/s` | `649272.961 ops/s` | `+12.30%` | `better` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:getWorkload` | `564496.135 ops/s` | `634067.383 ops/s` | `+12.32%` | `better` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:putWorkload` | `13666.544 ops/s` | `15205.578 ops/s` | `+11.26%` | `better` |
