# Benchmark Comparison

- Profile: `segment-index-pr-smoke`
- Baseline SHA: `1912aeb3cdf5d5e5748b841b244c8640aad43d62`
- Candidate SHA: `6526d87259948612cba98e2d76f05308f32cccd6`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `segment-index-get-live:getHitSync` | `2333416.742 ops/s` | `2571614.443 ops/s` | `+10.21%` | `better` |
| `segment-index-get-live:getMissSync` | `1974943.614 ops/s` | `1967021.256 ops/s` | `-0.40%` | `neutral` |
| `segment-index-get-persisted:getHitSync` | `1813272.560 ops/s` | `1812208.206 ops/s` | `-0.06%` | `neutral` |
| `segment-index-get-persisted:getMissSync` | `2232077.156 ops/s` | `2213924.764 ops/s` | `-0.81%` | `neutral` |
| `segment-index-hot-route-put:putHotRoute` | `2105016.248 ops/s` | `2092000.290 ops/s` | `-0.62%` | `neutral` |
| `segment-index-hot-route-put:putThenGetHotRoute` | `1157250.264 ops/s` | `1106952.448 ops/s` | `-4.35%` | `warning` |
| `segment-index-mixed-drain:partitionedIngestMixed` | `300071.379 ops/s` | `292298.504 ops/s` | `-2.59%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed:getWorkload` | `142341.548 ops/s` | `146663.747 ops/s` | `+3.04%` | `better` |
| `segment-index-mixed-drain:partitionedIngestMixed:putWorkload` | `157729.831 ops/s` | `145634.757 ops/s` | `-7.67%` | `worse` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed` | `187245.262 ops/s` | `173093.562 ops/s` | `-7.56%` | `worse` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:getWorkload` | `173372.572 ops/s` | `159006.640 ops/s` | `-8.29%` | `worse` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:putWorkload` | `13872.690 ops/s` | `14086.922 ops/s` | `+1.54%` | `neutral` |
