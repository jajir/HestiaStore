# Benchmark Comparison

- Profile: `segment-index-pr-smoke`
- Baseline SHA: `e0267401c382ba6c3c6bdc8b5961a42a9a7cef02`
- Candidate SHA: `fa45fc1c67efcb9c1f87c10ef1b08ec07564bb9a`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `segment-index-get-live:getHitSync` | `2261474.217 ops/s` | `2475980.603 ops/s` | `+9.49%` | `better` |
| `segment-index-get-live:getMissSync` | `1896905.524 ops/s` | `2064203.112 ops/s` | `+8.82%` | `better` |
| `segment-index-get-persisted:getHitSync` | `2168777.543 ops/s` | `2166498.897 ops/s` | `-0.11%` | `neutral` |
| `segment-index-get-persisted:getMissSync` | `2244255.227 ops/s` | `2260128.927 ops/s` | `+0.71%` | `neutral` |
| `segment-index-hot-route-put:putHotRoute` | `2176954.925 ops/s` | `2106531.466 ops/s` | `-3.23%` | `warning` |
| `segment-index-hot-route-put:putThenGetHotRoute` | `1158367.985 ops/s` | `1097124.558 ops/s` | `-5.29%` | `warning` |
| `segment-index-mixed-drain:partitionedIngestMixed` | `-` | `499150.140 ops/s` | `-` | `new` |
| `segment-index-mixed-drain:partitionedIngestMixed:getWorkload` | `-` | `329801.424 ops/s` | `-` | `new` |
| `segment-index-mixed-drain:partitionedIngestMixed:putWorkload` | `-` | `169348.716 ops/s` | `-` | `new` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed` | `545384.803 ops/s` | `658082.041 ops/s` | `+20.66%` | `better` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:getWorkload` | `531998.419 ops/s` | `643260.627 ops/s` | `+20.91%` | `better` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:putWorkload` | `13386.384 ops/s` | `14821.413 ops/s` | `+10.72%` | `better` |
