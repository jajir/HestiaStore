# Benchmark Comparison

- Profile: `segment-index-pr-smoke`
- Baseline SHA: `050681c55cf12c88b63d434fbb3236e80666bd7b`
- Candidate SHA: `050410ccb15738b7e7d78ca5becc5e22db08e3a3`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `segment-index-get-live:getHitSync` | `2286375.738 ops/s` | `2086904.574 ops/s` | `-8.72%` | `worse` |
| `segment-index-get-live:getMissSync` | `2115506.147 ops/s` | `2100589.573 ops/s` | `-0.71%` | `neutral` |
| `segment-index-get-persisted:getHitSync` | `1917073.038 ops/s` | `1932596.925 ops/s` | `+0.81%` | `neutral` |
| `segment-index-get-persisted:getMissSync` | `2152292.163 ops/s` | `2076782.296 ops/s` | `-3.51%` | `warning` |
| `segment-index-hot-route-put:putHotRoute` | `2169641.560 ops/s` | `2001542.224 ops/s` | `-7.75%` | `worse` |
| `segment-index-hot-route-put:putThenGetHotRoute` | `1105975.606 ops/s` | `1070907.116 ops/s` | `-3.17%` | `warning` |
| `segment-index-mixed-drain:partitionedIngestMixed` | `299043.025 ops/s` | `306128.432 ops/s` | `+2.37%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed:getWorkload` | `148398.233 ops/s` | `123133.789 ops/s` | `-17.02%` | `worse` |
| `segment-index-mixed-drain:partitionedIngestMixed:putWorkload` | `150644.792 ops/s` | `182994.643 ops/s` | `+21.47%` | `better` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed` | `185554.687 ops/s` | `199692.533 ops/s` | `+7.62%` | `better` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:getWorkload` | `170529.777 ops/s` | `185552.575 ops/s` | `+8.81%` | `better` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:putWorkload` | `15024.910 ops/s` | `14139.958 ops/s` | `-5.89%` | `warning` |
