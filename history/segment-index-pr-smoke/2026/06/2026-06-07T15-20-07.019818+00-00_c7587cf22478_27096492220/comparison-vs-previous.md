# Benchmark Comparison

- Profile: `segment-index-pr-smoke`
- Baseline SHA: `050681c55cf12c88b63d434fbb3236e80666bd7b`
- Candidate SHA: `c7587cf224789239262f5a67fbe0c9962510a205`
- Thresholds: neutral `<= 3.0%`, fail `> 7.0%` regression

| Metric | Baseline | Candidate | Delta | Status |
| --- | ---: | ---: | ---: | --- |
| `segment-index-get-live:getHitSync` | `2843974.519 ops/s` | `3136010.918 ops/s` | `+10.27%` | `better` |
| `segment-index-get-live:getMissSync` | `2622117.260 ops/s` | `2668473.902 ops/s` | `+1.77%` | `neutral` |
| `segment-index-get-persisted:getHitSync` | `2721999.146 ops/s` | `2462546.328 ops/s` | `-9.53%` | `worse` |
| `segment-index-get-persisted:getMissSync` | `2701448.708 ops/s` | `2629817.751 ops/s` | `-2.65%` | `neutral` |
| `segment-index-hot-route-put:putHotRoute` | `2690232.450 ops/s` | `2879769.125 ops/s` | `+7.05%` | `better` |
| `segment-index-hot-route-put:putThenGetHotRoute` | `1455852.815 ops/s` | `1448808.037 ops/s` | `-0.48%` | `neutral` |
| `segment-index-mixed-drain:partitionedIngestMixed` | `372206.383 ops/s` | `339086.737 ops/s` | `-8.90%` | `worse` |
| `segment-index-mixed-drain:partitionedIngestMixed:getWorkload` | `157933.874 ops/s` | `122253.396 ops/s` | `-22.59%` | `worse` |
| `segment-index-mixed-drain:partitionedIngestMixed:putWorkload` | `214272.509 ops/s` | `216833.341 ops/s` | `+1.20%` | `neutral` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed` | `274285.250 ops/s` | `267596.006 ops/s` | `-2.44%` | `neutral` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:getWorkload` | `255631.492 ops/s` | `249554.160 ops/s` | `-2.38%` | `neutral` |
| `segment-index-mixed-split-heavy:partitionedIngestMixed:putWorkload` | `18653.759 ops/s` | `18041.846 ops/s` | `-3.28%` | `warning` |
