# Benchmark Comparison

- Profile: `segment-index-pr-smoke`
- Baseline SHA: `bf25586e3b048415aef5becf26dc7dd9bcda9866`
- Candidate SHA: `8402d6728c8b3d0957fe89ad3908037071cf38b2`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `segment-index-get-live:getHitSync` | `2783314.120 ops/s` | `2721328.891 ops/s` | `-2.23%` | `neutral` |
| `segment-index-get-live:getMissSync` | `2575516.646 ops/s` | `2577019.817 ops/s` | `+0.06%` | `neutral` |
| `segment-index-get-persisted:getHitSync` | `2503071.442 ops/s` | `2492501.682 ops/s` | `-0.42%` | `neutral` |
| `segment-index-get-persisted:getMissSync` | `2550054.703 ops/s` | `2659945.541 ops/s` | `+4.31%` | `better` |
| `segment-index-hot-route-put:putHotRoute` | `2759217.679 ops/s` | `2626512.796 ops/s` | `-4.81%` | `warning` |
| `segment-index-hot-route-put:putThenGetHotRoute` | `1429628.160 ops/s` | `1380664.452 ops/s` | `-3.42%` | `warning` |
| `segment-index-mixed-drain:partitionedIngestMixed` | `375453.484 ops/s` | `325565.879 ops/s` | `-13.29%` | `worse` |
| `segment-index-mixed-drain:partitionedIngestMixed:getWorkload` | `165617.623 ops/s` | `113408.962 ops/s` | `-31.52%` | `worse` |
| `segment-index-mixed-drain:partitionedIngestMixed:putWorkload` | `209835.861 ops/s` | `212156.917 ops/s` | `+1.11%` | `neutral` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed` | `246531.349 ops/s` | `258545.833 ops/s` | `+4.87%` | `better` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:getWorkload` | `228392.738 ops/s` | `240499.461 ops/s` | `+5.30%` | `better` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:putWorkload` | `18138.611 ops/s` | `18046.373 ops/s` | `-0.51%` | `neutral` |
