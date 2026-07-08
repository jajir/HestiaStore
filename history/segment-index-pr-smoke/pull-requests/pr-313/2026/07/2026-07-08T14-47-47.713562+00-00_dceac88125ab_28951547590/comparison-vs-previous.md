# Benchmark Comparison

- Profile: `segment-index-pr-smoke`
- Baseline SHA: `67bbf8e34d2b602923270722df932c97760ccab3`
- Candidate SHA: `dceac88125ab35d7486f6cf3102eb4801e03ef50`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `segment-index-get-live:getHitSync` | `1784230.682 ops/s` | `1797937.456 ops/s` | `+0.77%` | `neutral` |
| `segment-index-get-live:getMissSync` | `1737586.648 ops/s` | `1720431.045 ops/s` | `-0.99%` | `neutral` |
| `segment-index-get-persisted:getHitSync` | `1642926.467 ops/s` | `1658594.080 ops/s` | `+0.95%` | `neutral` |
| `segment-index-get-persisted:getMissSync` | `1753597.454 ops/s` | `1780378.551 ops/s` | `+1.53%` | `neutral` |
| `segment-index-hot-route-put:putHotRoute` | `2294766.792 ops/s` | `2359191.748 ops/s` | `+2.81%` | `neutral` |
| `segment-index-hot-route-put:putThenGetHotRoute` | `1116462.131 ops/s` | `1200435.458 ops/s` | `+7.52%` | `better` |
| `segment-index-mixed-drain:partitionedIngestMixed` | `452502.829 ops/s` | `428908.602 ops/s` | `-5.21%` | `warning` |
| `segment-index-mixed-drain:partitionedIngestMixed:getWorkload` | `242630.955 ops/s` | `231117.959 ops/s` | `-4.75%` | `warning` |
| `segment-index-mixed-drain:partitionedIngestMixed:putWorkload` | `209871.874 ops/s` | `197790.643 ops/s` | `-5.76%` | `warning` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed` | `697470.708 ops/s` | `686910.413 ops/s` | `-1.51%` | `neutral` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:getWorkload` | `679201.279 ops/s` | `668610.923 ops/s` | `-1.56%` | `neutral` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:putWorkload` | `18269.430 ops/s` | `18299.490 ops/s` | `+0.16%` | `neutral` |
