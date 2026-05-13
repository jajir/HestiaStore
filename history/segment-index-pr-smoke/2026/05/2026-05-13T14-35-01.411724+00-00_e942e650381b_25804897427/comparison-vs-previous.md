# Benchmark Comparison

- Profile: `segment-index-pr-smoke`
- Baseline SHA: `7b4e3e01818862a8152f92756c8f59b19c59d7b3`
- Candidate SHA: `e942e650381bf00db7c2fbb3790ab0c49b708f39`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `segment-index-get-live:getHitSync` | `2272056.376 ops/s` | `2081784.893 ops/s` | `-8.37%` | `worse` |
| `segment-index-get-live:getMissSync` | `3616813.225 ops/s` | `3511394.039 ops/s` | `-2.91%` | `neutral` |
| `segment-index-get-multisegment-hot:getHitSync` | `7641.028 ops/s` | `7356.309 ops/s` | `-3.73%` | `warning` |
| `segment-index-get-multisegment-hot:getMissSync` | `3430230.512 ops/s` | `3603267.267 ops/s` | `+5.04%` | `better` |
| `segment-index-get-persisted:getHitSync` | `112937.122 ops/s` | `1869165.443 ops/s` | `+1555.05%` | `better` |
| `segment-index-get-persisted:getMissSync` | `3580894.076 ops/s` | `3284946.228 ops/s` | `-8.26%` | `worse` |
| `segment-index-hot-route-put:putHotRoute` | `2104905.414 ops/s` | `1953352.631 ops/s` | `-7.20%` | `worse` |
| `segment-index-hot-route-put:putThenGetHotRoute` | `1089046.778 ops/s` | `1071652.969 ops/s` | `-1.60%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed` | `274357.006 ops/s` | `308723.130 ops/s` | `+12.53%` | `better` |
| `segment-index-mixed-drain:partitionedIngestMixed:getWorkload` | `107929.409 ops/s` | `139234.777 ops/s` | `+29.01%` | `better` |
| `segment-index-mixed-drain:partitionedIngestMixed:putWorkload` | `166427.597 ops/s` | `169488.353 ops/s` | `+1.84%` | `neutral` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed` | `42182.687 ops/s` | `46365.393 ops/s` | `+9.92%` | `better` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:getWorkload` | `36857.082 ops/s` | `41059.250 ops/s` | `+11.40%` | `better` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:putWorkload` | `5325.605 ops/s` | `5306.143 ops/s` | `-0.37%` | `neutral` |
