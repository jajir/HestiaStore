# Benchmark Comparison

- Profile: `segment-index-pr-smoke`
- Baseline SHA: `8402d6728c8b3d0957fe89ad3908037071cf38b2`
- Candidate SHA: `7cc96a4f02588fc1e87970fc84af8f7132a59154`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `segment-index-get-live:getHitSync` | `2788335.684 ops/s` | `2834779.402 ops/s` | `+1.67%` | `neutral` |
| `segment-index-get-live:getMissSync` | `2659685.021 ops/s` | `2450877.569 ops/s` | `-7.85%` | `worse` |
| `segment-index-get-persisted:getHitSync` | `2461100.790 ops/s` | `2818970.853 ops/s` | `+14.54%` | `better` |
| `segment-index-get-persisted:getMissSync` | `2630255.058 ops/s` | `2573587.352 ops/s` | `-2.15%` | `neutral` |
| `segment-index-hot-route-put:putHotRoute` | `2705927.103 ops/s` | `2692289.073 ops/s` | `-0.50%` | `neutral` |
| `segment-index-hot-route-put:putThenGetHotRoute` | `1414022.675 ops/s` | `1440015.265 ops/s` | `+1.84%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed` | `343780.082 ops/s` | `336862.230 ops/s` | `-2.01%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed:getWorkload` | `116519.238 ops/s` | `111334.236 ops/s` | `-4.45%` | `warning` |
| `segment-index-mixed-drain:partitionedIngestMixed:putWorkload` | `227260.844 ops/s` | `225527.994 ops/s` | `-0.76%` | `neutral` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed` | `256283.163 ops/s` | `311315.189 ops/s` | `+21.47%` | `better` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:getWorkload` | `236403.774 ops/s` | `293755.134 ops/s` | `+24.26%` | `better` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:putWorkload` | `19879.389 ops/s` | `17560.054 ops/s` | `-11.67%` | `worse` |
